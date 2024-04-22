import os
import json
import time
import boto3
from faker import Faker
from config.config import configuration



# Stream Setup
os.environ['aws_access_key_id'] = configuration.get("AWS_ACCESS_KEY")
os.environ['aws_secret_access_key'] = configuration.get("AWS_SECRET_KEY")
os.environ['aws_region'] = configuration.get("AWS_REGION")
stream_name = configuration.get("STREAM_NAME")

# Initialize Faker and Boto3 Kinesis client
fake = Faker()
kinesis_client = boto3.client('kinesis',
                              aws_access_key_id=configuration.get("AWS_ACCESS_KEY"),
                              aws_secret_access_key=configuration.get("AWS_SECRET_KEY"),
                              region_name=configuration.get("AWS_REGION"))

# Directory setup
base_directory = os.path.dirname(os.path.dirname(__file__))
data_directory = os.path.join(base_directory, "data")


# Reading from stream functions
def get_shard_iterator():
    response = kinesis_client.get_shard_iterator(
        StreamName=stream_name,
        ShardID='shardId-000000000001',
        ShardIteratorType='TRIM_HORIZON'
    )
    return response['ShardIterator']

def get_records(shard_iterator):
    response = kinesis_client.get_records(
        ShardIterator=shard_iterator,
        Limit=10
    )
    return response.get('Records', []), response.get('NextShardIterator')

def process_data(data):
    print(f"Received Food Orders data: {data}")


if __name__ == "main":
    shard_iterator = get_shard_iterator()

    while True:
        records, next_shard_iterator = get_records(shard_iterator)
        for record in records:
            data = json.loads(record['Data'])
            process_data(data)

        if not next_shard_iterator:                                                                                     # Check if there's another record to read
            print("No more records found")
            break

        shard_iterator = next_shard_iterator
        time.sleep(1)

