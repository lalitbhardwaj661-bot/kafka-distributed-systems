"""
consumer.py - Kafka Consumer for Temperature Sensor Data
Subscribes to the 'sensor_data' topic and processes/prints incoming telemetry events.
"""

import json
import logging
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SensorConsumer")

DEFAULT_TOPIC = "sensor_data"
DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "temperature_monitor_group")


def parse_sensor_message(raw_message):
    """
    Parses raw message payload (bytes or string) into a structured Python dictionary.

    Args:
        raw_message (bytes or str or dict): Incoming message value.

    Returns:
        dict: Parsed telemetry event dictionary.
    """
    if isinstance(raw_message, dict):
        return raw_message
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8")
    return json.loads(raw_message)


def process_sensor_event(event):
    """
    Processes and prints formatted sensor reading, flagging warning conditions.

    Args:
        event (dict): Telemetry data containing sensor_id, timestamp, temperature, unit, status.

    Returns:
        str: Formatted log string.
    """
    sensor_id = event.get("sensor_id", "UNKNOWN")
    temperature = event.get("temperature", 0.0)
    unit = event.get("unit", "Celsius")
    timestamp = event.get("timestamp", "N/A")
    status = event.get("status", "NORMAL")

    alert_prefix = "⚠️ [ALERT]" if status == "WARNING" or temperature >= 29.0 else "✅ [OK]"
    output = (
        f"{alert_prefix} Sensor '{sensor_id}' | "
        f"Temperature: {temperature:.2f} {unit} | "
        f"Status: {status} | Timestamp: {timestamp}"
    )
    print(output, flush=True)
    return output


def get_kafka_consumer(topic=DEFAULT_TOPIC, bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS, group_id=DEFAULT_GROUP_ID):
    """
    Initializes and returns a Kafka consumer client.
    Supports either kafka-python or confluent-kafka.
    """
    try:
        from kafka import KafkaConsumer
        logger.info(f"Connecting to Kafka via kafka-python at {bootstrap_servers} for topic '{topic}'...")
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )
        return ("kafka-python", consumer)
    except ImportError:
        pass

    try:
        from confluent_kafka import Consumer
        logger.info(f"Connecting to Kafka via confluent-kafka at {bootstrap_servers} for topic '{topic}'...")
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True
        }
        consumer = Consumer(conf)
        consumer.subscribe([topic])
        return ("confluent-kafka", consumer)
    except ImportError:
        logger.error(
            "Neither 'kafka-python' nor 'confluent-kafka' library was found. "
            "Please install one: 'pip install kafka-python' or 'pip install confluent-kafka'."
        )
        raise ImportError("No Kafka client library available.")


def run_consumer(
    topic=DEFAULT_TOPIC,
    bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS,
    group_id=DEFAULT_GROUP_ID,
    max_messages=None
):
    """
    Subscribes to topic and prints incoming temperature events.
    """
    logger.info(f"Starting Sensor Consumer on topic='{topic}' (group='{group_id}')...")
    consumer_type, consumer = get_kafka_consumer(topic, bootstrap_servers, group_id)
    logger.info(f"Connected using {consumer_type}. Waiting for incoming events...")

    message_count = 0
    try:
        if consumer_type == "kafka-python":
            for message in consumer:
                payload = message.value
                process_sensor_event(payload)
                message_count += 1
                if max_messages and message_count >= max_messages:
                    break
        elif consumer_type == "confluent-kafka":
            while max_messages is None or message_count < max_messages:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.warning(f"Consumer error: {msg.error()}")
                    continue
                payload = parse_sensor_message(msg.value())
                process_sensor_event(payload)
                message_count += 1
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user. Shutting down gracefully...")
    finally:
        if consumer_type == "confluent-kafka":
            consumer.close()
        logger.info(f"Consumer stopped. Total messages processed: {message_count}")


if __name__ == "__main__":
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)
    topic_name = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)
    consumer_group = os.getenv("KAFKA_GROUP_ID", DEFAULT_GROUP_ID)
    run_consumer(topic=topic_name, bootstrap_servers=servers, group_id=consumer_group)
