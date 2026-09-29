"""
test_project.py - Unit Tests for Cloud & Distributed Systems Kafka Assessment
Uses pytest to validate:
 1. Sensor data keys
 2. Temperature threshold range and status logic
 3. Topic string specifications
 4. Output data types
 5. Consumer message parsing and processing
"""

import re
from datetime import datetime
import pytest

from producer import (
    generate_sensor_data,
    DEFAULT_TOPIC,
    MIN_TEMPERATURE,
    MAX_TEMPERATURE,
    HIGH_TEMP_THRESHOLD
)
from consumer import (
    DEFAULT_TOPIC as CONSUMER_TOPIC,
    parse_sensor_message,
    process_sensor_event
)


# ============================================================================
# Test 1: Validate Sensor Data Keys
# ============================================================================
def test_sensor_data_keys():
    """
    Test 1: Validate that the generated sensor data contains all required keys.
    Required keys: sensor_id, timestamp, temperature, unit, status.
    """
    required_keys = {"sensor_id", "timestamp", "temperature", "unit", "status"}
    data = generate_sensor_data()

    assert isinstance(data, dict), "Sensor data must be a dictionary"
    assert required_keys.issubset(data.keys()), (
        f"Missing required keys in sensor data: {required_keys - set(data.keys())}"
    )
    # Ensure there are no unexpected extra keys
    assert set(data.keys()) == required_keys, (
        f"Unexpected keys in sensor data: {set(data.keys()) - required_keys}"
    )


# ============================================================================
# Test 2: Validate Temperature Threshold Range
# ============================================================================
def test_temperature_threshold_range():
    """
    Test 2: Validate that generated temperature is strictly within designated bounds
    and that status matches threshold trigger condition (HIGH_TEMP_THRESHOLD).
    """
    # Sample multiple readings to verify statistical boundaries
    for _ in range(50):
        data = generate_sensor_data()
        temp = data["temperature"]
        status = data["status"]

        # Check bounds
        assert MIN_TEMPERATURE <= temp <= MAX_TEMPERATURE, (
            f"Temperature {temp}°C out of range [{MIN_TEMPERATURE}, {MAX_TEMPERATURE}]"
        )

        # Check status threshold logic
        if temp >= HIGH_TEMP_THRESHOLD:
            assert status == "WARNING", (
                f"Expected status 'WARNING' for temp {temp} >= {HIGH_TEMP_THRESHOLD}, got '{status}'"
            )
        else:
            assert status == "NORMAL", (
                f"Expected status 'NORMAL' for temp {temp} < {HIGH_TEMP_THRESHOLD}, got '{status}'"
            )


# ============================================================================
# Test 3: Validate Topic Strings
# ============================================================================
def test_topic_strings():
    """
    Test 3: Validate Kafka topic name conformity.
    Topic must match 'sensor_data', be non-empty, and conform to Kafka topic naming rules.
    """
    # Verify default topic equality across producer and consumer
    assert DEFAULT_TOPIC == "sensor_data", f"Producer topic must be 'sensor_data', got '{DEFAULT_TOPIC}'"
    assert CONSUMER_TOPIC == "sensor_data", f"Consumer topic must be 'sensor_data', got '{CONSUMER_TOPIC}'"
    assert DEFAULT_TOPIC == CONSUMER_TOPIC, "Producer and Consumer topics must match exactly"

    # Kafka topic constraints: length <= 249, alphanumeric with '.', '_', '-'
    kafka_topic_pattern = r"^[a-zA-Z0-9._-]+$"
    assert len(DEFAULT_TOPIC) > 0, "Topic string cannot be empty"
    assert len(DEFAULT_TOPIC) <= 249, "Kafka topic cannot exceed 249 characters"
    assert re.match(kafka_topic_pattern, DEFAULT_TOPIC), (
        f"Topic name '{DEFAULT_TOPIC}' contains invalid Kafka characters"
    )


# ============================================================================
# Test 4: Validate Output Data Types
# ============================================================================
def test_sensor_data_types():
    """
    Test 4: Validate that every field in the sensor telemetry dictionary conforms
    to the expected strict data type and format.
    """
    data = generate_sensor_data(sensor_id="sensor_test_01")

    # sensor_id: str
    assert isinstance(data["sensor_id"], str), f"sensor_id should be str, got {type(data['sensor_id'])}"
    assert data["sensor_id"] == "sensor_test_01"

    # timestamp: str conforming to ISO 8601
    assert isinstance(data["timestamp"], str), f"timestamp should be str, got {type(data['timestamp'])}"
    try:
        # Validate timestamp parseability
        datetime.fromisoformat(data["timestamp"])
    except ValueError as e:
        pytest.fail(f"timestamp '{data['timestamp']}' is not a valid ISO 8601 string: {e}")

    # temperature: float
    assert isinstance(data["temperature"], (float, int)), (
        f"temperature should be float, got {type(data['temperature'])}"
    )

    # unit: str (must be Celsius)
    assert isinstance(data["unit"], str), f"unit should be str, got {type(data['unit'])}"
    assert data["unit"] == "Celsius", f"Expected unit 'Celsius', got '{data['unit']}'"

    # status: str ('NORMAL' or 'WARNING')
    assert isinstance(data["status"], str), f"status should be str, got {type(data['status'])}"
    assert data["status"] in ("NORMAL", "WARNING"), f"Invalid status: '{data['status']}'"


# ============================================================================
# Test 5 (Bonus): Consumer Parsing and Event Formatting
# ============================================================================
def test_consumer_message_parsing_and_formatting():
    """
    Bonus Test 5: Validate JSON byte parsing and event processing in consumer.py.
    """
    sample_payload = {
        "sensor_id": "sensor_05",
        "timestamp": "2026-09-29T12:00:00+00:00",
        "temperature": 30.5,
        "unit": "Celsius",
        "status": "WARNING"
    }

    # Test bytes decoding and JSON loading
    raw_bytes = b'{"sensor_id": "sensor_05", "timestamp": "2026-09-29T12:00:00+00:00", "temperature": 30.5, "unit": "Celsius", "status": "WARNING"}'
    parsed = parse_sensor_message(raw_bytes)
    assert parsed == sample_payload

    # Test processing and alert formatting
    log_output = process_sensor_event(parsed)
    assert "[ALERT]" in log_output
    assert "sensor_05" in log_output
    assert "30.50 Celsius" in log_output
