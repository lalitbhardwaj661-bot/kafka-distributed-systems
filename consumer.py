import time
import json
import random
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import os

# 1. Render Port Requirement Fix (Background Dummy Web Server)
class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"IoT Kafka Consumer Service is Running Live!")

def run_dummy_server():
    port = int(os.environ.get("PORT", 10000))
    server_address = ('', port)
    httpd = HTTPServer(server_address, SimpleHTTPRequestHandler)
    print(f"Web server started on port {port}")
    httpd.serve_forever()

# Start Web Server in Background Thread for Render
threading.Thread(target=run_dummy_server, daemon=True).start()

# 2. Main Consumer Logic
print("Starting IoT Temperature Monitor Consumer...")

kafka_connected = False
consumer = None

try:
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        'sensor_data',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        request_timeout_ms=3000
    )
    kafka_connected = True
    print("Successfully connected to Kafka Broker!")
except Exception as e:
    print(f"Warning: Real Kafka broker not reachable ({e}).")
    print("Switching to Viva Demo / Simulation Mode...\n")

if kafka_connected and consumer:
    for message in consumer:
        data = message.value
        print(f"[KAFKA LIVE] Sensor ID: {data.get('sensor_id')} | Temp: {data.get('temperature')}°C | Status: {data.get('status')}")
else:
    sensors = ["SENSOR_ROOM_101", "SENSOR_LAB_2", "SENSOR_SERVER_ROOM"]
    while True:
        sensor_id = random.choice(sensors)
        temp = round(random.uniform(20.0, 45.0), 2)
        status = "CRITICAL HIGH" if temp > 40.0 else "NORMAL"
        
        log_entry = {
            "sensor_id": sensor_id,
            "temperature": temp,
            "unit": "Celsius",
            "status": status,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        print(f"[DEMO SIMULATION] Received Data -> {json.dumps(log_entry)}")
        time.sleep(3)
