import socket
import json
import time
import random
from faker import Faker

fake = Faker()
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(("localhost", 9999))
server_socket.listen(1)
print("📡 Waiting for Spark to connect...")

conn, addr = server_socket.accept()
print(f"✅ Connected to Spark from {addr}")

while True:
    data = {
        "patient_id": random.randint(1, 100),
        "timestamp": fake.iso8601(),
        "heart_rate": random.randint(60, 120),
        "blood_pressure": f"{random.randint(90, 140)}/{random.randint(60, 90)}",
        "oxygen_saturation": random.randint(90, 100)
    }
    message = json.dumps(data) + "\n"
    conn.send(message.encode("utf-8"))
    time.sleep(1)  # 1 record per second
