from data_generator_2 import RetailDataGenerator
from kafka import KafkaProducer
import boto3
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import logging
from io import BytesIO

stream_server = [
    'localhost:9092'
    ]

s3_client = boto3.client('s3')

producer = KafkaProducer(
    bootstrap_servers = stream_server,
    value_serializer = lambda v: json.dumps(v, default=str).encode('utf-8'),
    acks = 'all',
    retries=3,
    compression_type = 'gzip'
    )

s3_buffer = []
stats = {
            'total': 0,
            'kafka_today': 0,
            's3_historical': 0,
            's3_null_timestamp': 0,
            's3_future': 0,
            's3_files_written': 0,
            'kafka_failed': 0
        }    

def flatten_transactions(transactions):

   base_txn = {k:v for k,v in transactions.items() if k !='items'}
   return base_txn

if __name__=='__main__':

    transaction_date = datetime(2026,3,21)

    retail_generator = RetailDataGenerator(folder_path='/mnt/d/Projects/Project-2-Retail-Analytics-Pipeline/tmp/master_data')

    data = retail_generator.generate_daily_transactions(transaction_date)

    b = flatten_transactions(data)

    for item in b:
        for k,v in item.items():
            print(f'{k}:{v}')