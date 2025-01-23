import json
import gzip
import base64
from confluent_kafka import Producer
import sys
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def compress_and_encode_data(data):
    """
    Compress JSON data and encode to base64
    
    Args:
        data (dict): JSON data to compress
    
    Returns:
        str: Compressed and base64 encoded string
    """
    # Convert to JSON string
    json_str = json.dumps(data, separators=(',', ':'))
    
    # Compress using gzip
    compressed_data = gzip.compress(json_str.encode('utf-8'))
    
    # Encode to base64
    return base64.b64encode(compressed_data).decode('utf-8')

def read_json_and_send(json_file_path, topic, bootstrap_servers):
    """
    Read JSON file, compress, and send to Kafka topic
    
    Args:
        json_file_path (str): Path to the input JSON file
        topic (str): Kafka topic to send data to
        bootstrap_servers (str): Kafka broker address
    """
    # Kafka producer configuration with increased size limits
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
        # 'max.request.size': 20971520,  # 20MB request size
        'message.max.bytes': 20971520  # 20MB message size
    }
    
    # Create Kafka producer
    producer = Producer(producer_config)
    
    try:
        # Read JSON file
        with open(json_file_path, 'r') as file:
            data = json.load(file)
        
        # Compress and encode data
        encoded_data = compress_and_encode_data(data)
        
        # Send to Kafka topic
        producer.produce(topic, value=encoded_data.encode('utf-8'))
        
        # Flush to ensure message is sent
        producer.flush()
        
        print(f"Successfully sent compressed data from {json_file_path} to topic {topic}")
    
    except FileNotFoundError:
        print(f"Error: File {json_file_path} not found")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {json_file_path}")
    except Exception as e:
        print(f"Error sending data: {e}")
    finally:
        producer.flush()

# Consumer-side decoding function (for reference)
def decode_message(encoded_message):
    """
    Decode and decompress message on consumer side
    
    Args:
        encoded_message (str): Base64 encoded compressed message
    
    Returns:
        dict: Decoded and decompressed JSON data
    """
    # Decode base64
    compressed_data = base64.b64decode(encoded_message)
    
    # Decompress
    decompressed_data = gzip.decompress(compressed_data)
    
    # Parse JSON
    return json.loads(decompressed_data.decode('utf-8'))

# Example usage
if __name__ == "__main__":
    json_file_path = "/home/nikhil/Downloads/rn-tech-interview-pt2/rn-tech-interview-pt2/rollup-request-2025-01-22 16:17:15.326991.json"
    topic = os.getenv("INPUT_KAFKA_TOPIC")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    
    read_json_and_send(json_file_path, topic, bootstrap_servers)