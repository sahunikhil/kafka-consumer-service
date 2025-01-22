import os
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Consumer, KafkaException, KafkaError
from dotenv import load_dotenv
import multiprocessing
import time

# Load environment variables from .env file
load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def process_message(message):
    """
    Simulate a long-running CPU-intensive task.
    """
    try:
        logger.info(f"Processing message: {message}")
        # Simulate a blocking task (e.g., heavy computation or I/O delay)
        time.sleep(5)  # Simulate heavy task with sleep
        logger.info(f"Finished processing message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

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

def consume_messages():
    """
    Consume messages from the Kafka topic and process them concurrently.
    """
    try:
        consumer = create_consumer()
        consumer.subscribe([KAFKA_TOPIC])
        
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
                        value = json.loads(msg.value().decode('utf-8'))
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode message: {e}")
                        continue
                    
                    # Submit message for processing
                    executor.submit(process_message, value)
                    
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
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")

if __name__ == "__main__":
    consume_messages()