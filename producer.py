"""
producer.py - Kafka Producer for Temperature Sensor Data
Generates mock temperature sensor telemetry and publishes to the 'sensor_data' Kafka topic.
"""

import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SensorProducer")

# Configuration constants
DEFAULT_TOPIC = "sensor_data"
DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_INTERVAL_SECONDS = 2.0
MIN_TEMPERATURE = 18.0
MAX_TEMPERATURE = 32.0
HIGH_TEMP_THRESHOLD = 29.0


def generate_sensor_data(sensor_id=None, min_temp=MIN_TEMPERATURE, max_temp=MAX_TEMPERATURE):
    """
    Generates a mock temperature sensor reading dictionary.

    Returns:
        dict: Telemetry data payload with keys:
              sensor_id, timestamp, temperature, unit, status
    """
    if sensor_id is None:
        sensor_id = f"sensor_{random.randint(1, 10):02d}"

    temperature = round(random.uniform(min_temp, max_temp), 2)
    status = "WARNING" if temperature >= HIGH_TEMP_THRESHOLD else "NORMAL"

    return {
        "sensor_id": str(sensor_id),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": float(temperature),
        "unit": "Celsius",
        "status": str(status)
    }


def get_kafka_producer(bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS):
    """
    Initializes and returns a Kafka producer client.
    Supports either kafka-python or confluent-kafka.
    """
    try:
        from kafka import KafkaProducer
        logger.info(f"Connecting to Kafka via kafka-python at {bootstrap_servers}...")
        return (
            "kafka-python",
            KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3
            )
        )
    except ImportError:
        pass

    try:
        from confluent_kafka import Producer
        logger.info(f"Connecting to Kafka via confluent-kafka at {bootstrap_servers}...")
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "sensor-producer",
            "acks": "all"
        }
        return ("confluent-kafka", Producer(conf))
    except ImportError:
        logger.error(
            "Neither 'kafka-python' nor 'confluent-kafka' library was found. "
            "Please install one: 'pip install kafka-python' or 'pip install confluent-kafka'."
        )
        raise ImportError("No Kafka client library available.")


def send_message(producer_type, producer, topic, payload):
    """
    Publishes a single JSON payload to the specified topic using the active producer.
    """
    if producer_type == "kafka-python":
        producer.send(topic, value=payload)
        producer.flush()
    elif producer_type == "confluent-kafka":
        producer.produce(
            topic=topic,
            value=json.dumps(payload).encode("utf-8")
        )
        producer.flush()
    else:
        raise ValueError(f"Unsupported producer type: {producer_type}")


def run_producer(
    topic=DEFAULT_TOPIC,
    bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS,
    interval=DEFAULT_INTERVAL_SECONDS,
    max_messages=None
):
    """
    Main producer loop emitting mock temperature sensor readings every `interval` seconds.
    """
    logger.info(f"Starting Sensor Producer targeting topic='{topic}', bootstrap='{bootstrap_servers}'...")
    producer_type, producer = get_kafka_producer(bootstrap_servers)
    logger.info(f"Connected using {producer_type}. Publishing readings every {interval}s...")

    count = 0
    try:
        while max_messages is None or count < max_messages:
            data = generate_sensor_data()
            send_message(producer_type, producer, topic, data)
            count += 1
            logger.info(f"[#{count}] Published to {topic}: {data}")
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user. Shutting down gracefully...")
    finally:
        logger.info(f"Producer finished. Total messages sent: {count}")


if __name__ == "__main__":
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)
    topic_name = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)
    run_producer(topic=topic_name, bootstrap_servers=servers, interval=DEFAULT_INTERVAL_SECONDS)
