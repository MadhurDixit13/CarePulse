import boto3
import json
import time
import random
from faker import Faker
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")

fake = Faker()

# Create Kinesis client
client = boto3.client('kinesis',
                      region_name=AWS_REGION,
                      aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY)

while True:
    data = {
        "patient_id": random.randint(1, 100),
        "timestamp": fake.iso8601(),
        "heart_rate": random.randint(60, 120),
        "blood_pressure": f"{random.randint(90, 140)}/{random.randint(60, 90)}",
        "oxygen_saturation": random.randint(90, 100)
    }

    json_data = json.dumps(data)

    response = client.put_record(
        StreamName="vitals-stream",
        Data=json_data,
        PartitionKey=str(data["patient_id"])
    )

    print(f"Sent: {json_data} | ShardID: {response['ShardId']}")
    time.sleep(1)
