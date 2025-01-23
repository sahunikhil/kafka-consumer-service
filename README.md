# Kafka Consumer Service

## Overview
A Python service that consumes messages from a Kafka topic and processes them concurrently. The service uses the confluent-kafka client and implements a thread pool executor to handle long-running tasks without blocking message consumption.

## Features
- Kafka message consumption using confluent-kafka client
- Concurrent message processing using ThreadPoolExecutor
- CPU core-based thread allocation
- Manual offset commitment
- Comprehensive error handling and logging
- Simulated long-running task processing

## Prerequisites
- Python 3.8+
- Apache Kafka 3.9.0
- Kafka cluster (local or remote)

## Installation

1. Clone the repository:
```bash
git clone <repository_url>
cd kafka-consumer-service
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment variables:
```bash
cp .env.example .env
```

Update `.env` with your configuration:
```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_GROUP_ID=your-consumer-group
KAFKA_TOPIC=Scenario-Execute
```

## Usage

1. Start the consumer service:
```bash
python main.py
```

2. To test, use the Kafka console producer:
```bash
kafka-console-producer --broker-list localhost:9092 --topic Scenario-Execute
```

3. Send test messages:
```json
{"scenario": "run-analysis", "parameters": {"analysisType": "quick", "threshold": 10}, "timestamp": 1673456734}
```

## Implementation Details

### Message Processing
The service implements three main functions:

1. `process_message(message)`:
   - Simulates a long-running task using `time.sleep(5)`
   - Logs start and completion of message processing
   - Includes error handling for processing failures

2. `create_consumer()`:
   - Creates a configured Kafka consumer instance
   - Configuration settings:
     ```python
     {
         'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
         'group.id': KAFKA_GROUP_ID,
         'auto.offset.reset': 'earliest',
         'enable.auto.commit': False,
         'session.timeout.ms': 60000,
         'max.poll.interval.ms': 300000
     }
     ```

3. `consume_messages()`:
   - Main consumer loop with concurrent message processing
   - Uses ThreadPoolExecutor with CPU core count for worker threads
   - Implements manual offset commitment
   - Handles various error conditions:
     - Partition EOF
     - Message decoding errors
     - General processing errors
     - Shutdown errors

### Concurrency Approach
- Assuming the task is I/O-bound task thats why using thread pool
- Uses `ThreadPoolExecutor` for concurrent message processing
- Number of worker threads equals available CPU cores
- Each message is processed in a separate thread
- Non-blocking message consumption while processing occurs

### Error Handling
The service implements comprehensive error handling:
- JSON decode errors for malformed messages
- Kafka-specific errors (partition EOF, consumer errors)
- Processing errors within message handling
- Graceful shutdown handling

## Dependencies
```
confluent-kafka==2.8.0
python-dotenv==1.0.1
```

## Sample Output
```
2025-01-22 13:53:42,183 - INFO - Connected to Kafka and listening to the topic...
2025-01-22 13:53:42,184 - INFO - Using 8 threads for concurrent processing.
2025-01-22 13:53:42,308 - INFO - Processing message: {'scenario': 'run-analysis', 'parameters': {'analysisType': 'quick', 'threshold': 10}, 'timestamp': 1673456734}
2025-01-22 13:53:47,308 - INFO - Finished processing message: {'scenario': 'run-analysis', 'parameters': {'analysisType': 'quick', 'threshold': 10}, 'timestamp': 1673456734}
```

## Limitations and Considerations

### Threading Limitations
- Python's GIL may limit true parallelism for CPU-bound tasks
- Current implementation uses threads which are better suited for I/O-bound tasks
- For CPU-intensive tasks, consider using `ProcessPoolExecutor`

### Production Considerations
1. Message Processing:
   - Implement actual processing logic instead of sleep
   - Add message validation
   - Implement retry mechanisms
   - Add dead letter queue for failed messages

2. Monitoring and Scaling:
   - Add metrics collection
   - Monitor consumer lag
   - Implement health checks
   - Consider horizontal scaling with multiple consumers

3. Error Handling:
   - Implement more sophisticated error recovery
   - Add alerting for critical failures
   - Consider implementing circuit breakers

### Scaling
- Ensure proper topic partitioning
- Consider consumer group configuration
- Monitor and adjust thread pool size based on performance metrics

---
---
# Technical round 2

# Kafka Weekly Aggregation Consumer

## Overview
Enhanced Kafka consumer service that aggregates hourly data into weekly summaries across different dimensions.

## Features
- Receives hourly data from Scenario-Execute topic
- Aggregates data by Contact Type, Staff Type, and Call Center
- Sends aggregated results to Scenario-Execute-Response topic
- Optional Parquet serialization of weekly data

## Installation
```bash
pip install -r requirements.txt
```

## Configuration
Update `.env` with:
```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_GROUP_ID=weekly-aggregator
INPUT_KAFKA_TOPIC=Scenario-Execute
OUTPUT_KAFKA_TOPIC=Scenario-Execute-Response
```

## Aggregation Rules
- Volume: Sum
- Abandons: Sum
- Top Line Agents (FTE): Sum
- Base AHT: Average
- Handled Threshold: Sum
- Service Level: Average
- Acceptable Wait Time: Average
- Total Queue Time: Sum

## Challenges Faced
1. Data too big for topic, so compressing and encoding (base64) then sending to Kafka.
2. Dynamic data aggregation across multiple dimensions
3. Implementing flexible week start configuration

## Testing
1. Prepare test JSON payload
2. Produce message to Scenario-Execute topic
3. Verify aggregated message in Scenario-Execute-Response topic

## Bonus Implementations
- Dynamic week start day support
- Parquet serialization of weekly data
- Robust error handling

## Performance Considerations
- Uses pandas for efficient data manipulation
- Concurrent message processing
- Minimal memory overhead