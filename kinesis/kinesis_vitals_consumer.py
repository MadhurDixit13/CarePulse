import boto3
import json
import time
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
# AWS setup
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
STREAM_NAME = os.getenv("STREAM_NAME")
SHARD_ITERATOR_TYPE = "LATEST"

# Output folder for Parquet batches
output_dir = Path("kinesis_data")
output_dir.mkdir(exist_ok=True)

# Initialize Kinesis client
client = boto3.client('kinesis',
                      region_name=AWS_REGION,
                      aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY)

# Step 1: Get shard ID
stream = client.describe_stream(StreamName=STREAM_NAME)
shard_id = stream['StreamDescription']['Shards'][0]['ShardId']

# Step 2: Get shard iterator
response = client.get_shard_iterator(
    StreamName=STREAM_NAME,
    ShardId=shard_id,
    ShardIteratorType=SHARD_ITERATOR_TYPE
)
shard_iterator = response['ShardIterator']

print("📡 Starting to pull data from Kinesis...")

records_buffer = []

while True:
    # Get records
    response = client.get_records(ShardIterator=shard_iterator, Limit=100)
    shard_iterator = response['NextShardIterator']
    records = response['Records']

    # Extract and decode
    for r in records:
        raw = json.loads(r['Data'])
        records_buffer.append(raw)

    # Write every 100 records
    if len(records_buffer) >= 100:
        df = pd.DataFrame(records_buffer)
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        df.to_parquet(output_dir / f"vitals_{ts}.parquet", index=False)
        print(f"✅ Wrote {len(records_buffer)} records at {ts}")
        records_buffer = []

    time.sleep(1)
