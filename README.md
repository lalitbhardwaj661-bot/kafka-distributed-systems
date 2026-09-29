# Cloud & Distributed Systems Assessment: Event-Driven Kafka Telemetry Pipeline

An end-to-end distributed telemetry streaming pipeline built with **Apache Kafka**, **Apache ZooKeeper**, and **Python**. This project simulates an IoT temperature sensor network streaming data in real-time to a distributed event broker, consumed and monitored by an alerting consumer service, verified with automated unit tests and GitHub Actions CI.

---

## Architecture Diagram

```
+-----------------------------------------------------------------------------------+
|                        Distributed Telemetry Architecture                         |
+-----------------------------------------------------------------------------------+

   +--------------------------+
   |   Mock IoT Temperature   |
   |         Sensors          |
   +-------------+------------+
                 |
                 v
   +--------------------------+
   |       producer.py        |
   |   - Generates reading    |
   |   - Checks thresholds    |
   |   - Emits every 2s       |
   +-------------+------------+
                 |
                 | (Port 9092) TCP / JSON payload
                 v
   +-------------------------------------------------------------+
   |                  Docker Compose Environment                 |
   |                                                             |
   |   +-----------------------+     +-----------------------+   |
   |   |       ZooKeeper       | <-> |      Kafka Broker     |   |
   |   |     (Port: 2181)      |     |     (Port: 9092)      |   |
   |   | Cluster coordination  |     | Topic: 'sensor_data'  |   |
   |   +-----------------------+     +-----------+-----------+   |
   +---------------------------------------------|---------------+
                                                 |
                                                 | Subscribes to 'sensor_data'
                                                 v
                                   +---------------------------+
                                   |        consumer.py        |
                                   |   - Deserializes JSON     |
                                   |   - Validates schema      |
                                   |   - Alerts on high temps  |
                                   |   - Prints live monitor   |
                                   +---------------------------+

   +-------------------------------------------------------------+
   |                     Automated Testing & CI                  |
   |                                                             |
   |  +--------------------+        +-------------------------+  |
   |  |  test_project.py   | -----> | GitHub Actions (ci.yml) |  |
   |  |  4+ Pytest tests   |        | On Push & Pull Request  |  |
   |  +--------------------+        +-------------------------+  |
   +-------------------------------------------------------------+
```

---

## Features

- **Event-Driven Distributed Messaging**: Uses Apache Kafka for durable, partitioned, low-latency publish-subscribe streaming.
- **ZooKeeper Cluster Management**: Coordinates broker metadata and health checks in isolated Docker containers.
- **Real-Time Sensor Simulation**: `producer.py` emits structured JSON telemetry events (sensor ID, timestamp, temperature, unit, status) every 2 seconds.
- **Dynamic Threshold Alerts**: Automatically flags temperature readings above 29.0°C with `WARNING` status and alert banners.
- **Live Stream Consumer**: `consumer.py` subscribes to the `sensor_data` topic, decodes byte payloads, tracks message offsets, and logs telemetry.
- **Comprehensive Unit Test Suite**: `test_project.py` with pytest covering:
  1. Sensor data schema and keys validation
  2. Temperature threshold boundaries & status transition logic
  3. Kafka topic naming standards and conformance
  4. Output data types and ISO 8601 timestamps
  5. Deserialization and formatted alert logging
- **Automated CI/CD**: `.github/workflows/ci.yml` runs pytest across multiple Python versions on every `push` and `pull_request`.

---

## Project Structure

```
.
├── .github/
│   └── workflows/
│       └── ci.yml               # GitHub Actions CI workflow
├── .gitignore                   # Ignored files (caches, virtualenvs)
├── docker-compose.yml           # ZooKeeper and Kafka service orchestration
├── producer.py                  # Kafka producer generating telemetry every 2s
├── consumer.py                  # Kafka consumer listening to sensor_data topic
├── test_project.py              # Unit tests validating keys, thresholds, topics, types
├── requirements.txt             # Python dependencies (pytest, kafka-python/ng)
└── README.md                    # Project documentation and architecture guide
```

---

## Prerequisites

1. **Docker & Docker Compose**: Ensure [Docker Desktop](https://www.docker.com/products/docker-desktop/) is installed and running.
2. **Python 3.10+**: Verify installation with `python --version`.
3. **Git**: Required for source control and GitHub CI integration.

---

## Step-by-Step Setup Instructions

### 1. Clone or Open the Repository
```bash
git clone <your-repo-url>
cd CC
```

### 2. Set Up Python Environment
Create and activate a virtual environment:

**On Windows (PowerShell):**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

**On Linux / macOS:**
```bash
python3 -m venv venv
source venv/bin/activate
```

Install the dependencies:
```bash
pip install -r requirements.txt
```

---

### 3. Start Kafka and ZooKeeper Services
Launch the distributed broker and ZooKeeper using Docker Compose:

```bash
docker compose up -d
```

Verify that both containers are healthy and running:
```bash
docker compose ps
```

You should see:
- `zookeeper` listening on port `2181`
- `kafka` listening on port `9092`

---

### 4. Start the Telemetry Consumer
In a new terminal window (with virtual environment activated):

```bash
python consumer.py
```

Expected output:
```text
2026-09-29 15:00:00,000 [INFO] Connecting to Kafka via kafka-python at localhost:9092 for topic 'sensor_data'...
2026-09-29 15:00:01,000 [INFO] Connected using kafka-python. Waiting for incoming events...
```

---

### 5. Start the Sensor Producer
In another terminal window:

```bash
python producer.py
```

Expected output (emitted every 2 seconds):
```text
2026-09-29 15:00:02,000 [INFO] Starting Sensor Producer targeting topic='sensor_data'...
2026-09-29 15:00:03,000 [INFO] [#{1}] Published to sensor_data: {'sensor_id': 'sensor_04', 'timestamp': '2026-09-29T09:30:00.000000+00:00', 'temperature': 24.32, 'unit': 'Celsius', 'status': 'NORMAL'}
2026-09-29 15:00:05,000 [INFO] [#{2}] Published to sensor_data: {'sensor_id': 'sensor_09', 'timestamp': '2026-09-29T09:30:02.000000+00:00', 'temperature': 30.15, 'unit': 'Celsius', 'status': 'WARNING'}
```

Meanwhile, `consumer.py` will print real-time events:
```text
✅ [OK] Sensor 'sensor_04' | Temperature: 24.32 Celsius | Status: NORMAL | Timestamp: 2026-09-29T09:30:00.000000+00:00
⚠️ [ALERT] Sensor 'sensor_09' | Temperature: 30.15 Celsius | Status: WARNING | Timestamp: 2026-09-29T09:30:02.000000+00:00
```

---

### 6. Run Unit Tests with Pytest
Run the test suite locally in your terminal to verify all criteria:

```bash
pytest -v test_project.py
```

Sample output:
```text
============================= test session starts =============================
platform win32 -- Python 3.x.x, pytest-x.x.x
rootdir: c:\Users\DINESH BHARDWAJ\Downloads\CC
collected 5 items

test_project.py::test_sensor_data_keys PASSED                            [ 20%]
test_project.py::test_temperature_threshold_range PASSED                 [ 40%]
test_project.py::test_topic_strings PASSED                               [ 60%]
test_project.py::test_sensor_data_types PASSED                           [ 80%]
test_project.py::test_consumer_message_parsing_and_formatting PASSED     [100%]

============================== 5 passed in 0.05s ==============================
```

---

### 7. Teardown / Cleanup
To stop the Kafka and ZooKeeper containers:

```bash
docker compose down -v
```

---

## Assessment Criteria Coverage

| Requirement | Implementation | Validation |
| :--- | :--- | :--- |
| **Docker Compose** | `docker-compose.yml` defining ZooKeeper & Kafka | Ports 2181 & 9092 mapped, auto topic creation enabled |
| **Producer** | `producer.py` emitting telemetry every 2 seconds | Publishes to `sensor_data` with retry logic & JSON serialization |
| **Consumer** | `consumer.py` listening on `sensor_data` | Deserializes JSON, displays readings & triggers warning alerts |
| **Unit Tests (4+)** | `test_project.py` with 5 unit tests | Tests keys, threshold range, topic strings, and data types |
| **CI/CD** | `.github/workflows/ci.yml` | Multi-version Python matrix automated testing on push/PR |
