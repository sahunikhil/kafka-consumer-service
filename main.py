import os
import json
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Consumer, Producer, KafkaError
from dotenv import load_dotenv
import multiprocessing
from datetime import timedelta
from traceback import format_exc

from send_data_to_topic import decode_message, compress_and_encode_data

# Load environment variables from .env file
load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
INPUT_KAFKA_TOPIC = os.getenv("INPUT_KAFKA_TOPIC", "Scenario-Execute")
OUTPUT_KAFKA_TOPIC = os.getenv("OUTPUT_KAFKA_TOPIC", "Scenario-Execute-Response")

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def aggregate_weekly_data(weekly_data, week_start='friday'):
    """
    Aggregate weekly data with dynamic week calculation
    
    Args:
        weekly_data (list): List of dictionaries containing weekly data
        week_start (str, optional): First day of the week. Defaults to 'friday'.
    
    Returns:
        dict: Aggregated weekly data by group key
    """
    # Convert to DataFrame
    df = pd.DataFrame(weekly_data)
    
    # Create unique group key
    df['group_key'] = (
        df['Contact Type'] + '_' + 
        df['Staff Type'] + '_' + 
        df['Call Center']
    )
    
    # Convert date column to datetime
    df['Week'] = pd.to_datetime(df['Week'])
    
    # Function to get week start date
    def get_week_start(date):
        # Adjust to the specified week start day
        days_map = {
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 
            'thursday': 3, 'friday': 4, 'saturday': 5, 'sunday': 6
        }
        target_weekday = days_map[week_start.lower()]
        
        # Get the first occurrence of the week start day
        week_start_date = date - timedelta(days=(date.weekday() - target_weekday + 7) % 7)
        return week_start_date
    
    # Add week start column
    df['Week'] = df['Week'].apply(get_week_start)
    
    # Aggregation rules
    aggregation_rules = {
        'Volume': 'sum',
        'Abandons': 'sum',
        'Top Line Agents (FTE)': 'sum',
        'Base AHT': 'mean',
        'Handled Threshold': 'sum',
        'Service Level (X Seconds)': 'mean',
        'Acceptable Wait Time': 'mean',
        'Total Queue Time': 'sum'
    }
    
    # Group and aggregate by group_key and Week
    aggregated_df = df.groupby(['group_key', 'Week']).agg(aggregation_rules).reset_index()
    
    # Convert Week to string for JSON serialization
    aggregated_df['Week'] = aggregated_df['Week'].dt.strftime('%Y-%m-%d')
    
    # Group results by group_key
    result = {}
    for group, group_data in aggregated_df.groupby('group_key'):
        result[group] = group_data.drop('group_key', axis=1).to_dict(orient='records')
    # print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n", result, "\n$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    # with open('result.json', 'w') as f:
    #     json.dump(result, f)
    return result

def create_consumer():
    """
    Create and configure a Kafka consumer instance.
    """
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # Manual commit for better control
        'session.timeout.ms': 60000,  # Increase timeout for long-running tasks
        'max.poll.interval.ms': 300000  # Allow more time for message processing
    }
    
    return Consumer(config)

def create_producer():
    """Create Kafka producer"""
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
    }
    return Producer(config)

def delivery_report(err, msg):
    """Callback for message delivery"""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def process_message(message, producer, output_topic):
    """
    Process individual message and send aggregated response
    """
    try:
        logger.info(f"Processing message: {message}")
        
        # Check if message is a rollup event
        if isinstance(message, dict) and message.get('event') == 'rollup':
            # Perform aggregation
            weekly_aggregated = aggregate_weekly_data(
                message['weekly'], 
                message.get('week_start', 'friday')
            )
            
            # Prepare response payload
            response = {
                'event': 'rollup',
                'organizationId': message['organizationId'],
                'week_start': message.get('week_start', 'friday'),
                'weekly': weekly_aggregated
            }
            print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n", response, "\n$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
            with open('result.json', 'w') as f:
                json.dump(response, f)
            
            # Serialize to Parquet (optional bonus implementation)
            # df = pd.DataFrame.from_dict(response['weekly'], orient='index')[0]
            df = pd.DataFrame(response['weekly'])
            parquet_filename = f"weekly_rollup_{message['organizationId']}.parquet"
            df.to_parquet(parquet_filename)
            logger.info(f"Saved Parquet file: {parquet_filename}")
            df.to_json("weekly_rollup_response.json")

            # encode message before sending
            encoded_data = compress_and_encode_data(response)
            
            # Produce response to Kafka
            producer.produce(
                output_topic, 
                encoded_data.encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)
        else:
            # Simulate long-running task for non-rollup messages
            # time.sleep(5)  
            logger.info(f"Finished processing message: {message}")
        
    except Exception as e:
        logger.error(f"Error processing message: {format_exc()}")

def consume_messages():
    """
    Consume messages from the Kafka topic and process them concurrently.
    """
    try:
        consumer = create_consumer()
        producer = create_producer()
        consumer.subscribe([INPUT_KAFKA_TOPIC])
        
        logger.info("Connected to Kafka and listening to the topic...")
        
        # Determine the number of CPU cores to use for concurrency
        max_workers = multiprocessing.cpu_count()
        logger.info(f"Using {max_workers} threads for concurrent processing.")
        
        # Thread pool for handling message processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                try:
                    # Poll for messages with a timeout
                    msg = consumer.poll(1.0)
                    
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            logger.info(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                        else:
                            logger.error(f"Error while consuming message: {msg.error()}")
                        continue
                    
                    # Parse message value
                    try:
                        # Decode message
                        value = decode_message(msg.value())
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode message: {e}")
                        continue
                    
                    # Submit message for processing
                    executor.submit(process_message, value, producer, OUTPUT_KAFKA_TOPIC)
                    
                    # Commit offset after submitting for processing
                    consumer.commit(msg)
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    logger.error(f"Error processing message batch: {e}")
                    
    except Exception as e:
        logger.error(f"Error in consuming messages: {e}")
    finally:
        # Clean shutdown
        try:
            consumer.close()
            producer.flush()
        except Exception as e:
            logger.error(f"Error closing consumer/producer: {e}")

if __name__ == "__main__":
    consume_messages()