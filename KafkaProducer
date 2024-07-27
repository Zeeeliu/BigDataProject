from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
import json
import logging
import time

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('kafka')

# Create a Kafka producer with connection retry
def create_producer(max_retries=5, retry_interval=5):
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5,
                acks='all',
                request_timeout_ms=30000,  # 30 seconds
                retry_backoff_ms=500,  # 0.5 seconds
                connections_max_idle_ms=600000,  # 10 minutes
                max_in_flight_requests_per_connection=5,  # Limits the number of in-flight requests per connection
                linger_ms=100,  # Linger time to batch messages
                batch_size=16384  # Batch size for the producer
            )
            logger.info("Successfully connected to Kafka broker")
            return producer
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka broker (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)

    logger.error("Max retries reached. Unable to connect to Kafka broker.")
    return None

# Create a Kafka producer
producer = create_producer()

if producer is None:
    logger.error("Exiting due to connection failure")
    exit(1)

# Messages to send
messages = [
    {"SSN": "111-222-333", "Occupation": "pooper", "State": "California"},
]

# Send messages
for message in messages:
    logger.debug(f'Sending message: {message}')
    future = producer.send('test-topic', message)
    try:
        record_metadata = future.get(timeout=10)
        logger.debug(
            f'Message sent successfully. Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}')
    except KafkaTimeoutError:
        logger.error(f'Failed to send message due to timeout: {message}')
    except KafkaError as e:
        logger.error(f'Failed to send message: {str(e)}')

# Ensure all messages are sent before exiting
logger.debug('Flushing producer')
producer.flush()
logger.debug('Producer flush complete')
producer.close()
