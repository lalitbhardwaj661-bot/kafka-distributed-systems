import time
import json
import random
import threading
import os
from http.server import HTTPServer, BaseHTTPRequestHandler

# Background Port Server
class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"IoT Simulation Service Running Live")

def run_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(('0.0.0.0', port), SimpleHTTPRequestHandler)
    server.serve_forever()

threading.Thread(target=run_server, daemon=True).start()

# Live Continuous Simulation Loop
print("Starting IoT Temperature Monitor Consumer...", flush=True)

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
    
    print(f"[DEMO SIMULATION] Received Data -> {json.dumps(log_entry)}", flush=True)
    time.sleep(3)
