import os
import json
import time
import boto3
import random
import pandas as pd
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


def load_ids_from_csv(file_name, key_attribute):
    file_path = os.path.join(data_directory, file_name)
    df = pd.read_csv(file_path)
    return df[key_attribute].tolist()

def generate_order(customer_ids, restaurant_ids, driver_ids, order_id):
    order = {
        'OrderID': order_id,
        'CustomerID': random.choice(customer_ids),
        'RestaurantID': random.choice(restaurant_ids),
        'DriverID': random.choice(driver_ids),
        'OrderDate': fake.date_time_between(start_date='-30d', end_date='now').isoformat(),
        'DeliveryTime': random.randint(15, 60),  # Delivery time in minutes
        'OrderValue': round(random.uniform(10, 100), 2),
        'DeliveryFee': round(random.uniform(2, 10), 2),
        'TipAmount': round(random.uniform(0, 20), 2),
        'OrderStatus': random.choice(['Delivered', 'Cancelled', 'Processing', 'On the way'])
    }
    return order

def send_order_to_kinesis(stream_name, order):
    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(order),
        PartitionKey=str(order['OrderID'])                                                                                # Using OrderID as the partition key
    )
    print(order)
    print(f"Order sent to Kinesis Stream --- ShardID: {response['ShardId']} and Sequence Number: {response['SequenceNumber']}")


if __name__ == "__main__":
    # Load data IDs from files
    customer_ids = load_ids_from_csv('dimCustomers.csv', 'CustomerID')
    restaurant_ids = load_ids_from_csv('dimRestaurants.csv', 'RestaurantID')
    driver_ids = load_ids_from_csv('dimDeliveryDrivers.csv', 'DriverID')

    # Sending Order Data to Kinesis
    order_id = 1000
    for _ in range(1000):
        order = generate_order(customer_ids, restaurant_ids, driver_ids, order_id)
        send_order_to_kinesis(stream_name, order)
        order_id += 1                                                                                                         # Increment OrderID for the next order
        time.sleep(1)