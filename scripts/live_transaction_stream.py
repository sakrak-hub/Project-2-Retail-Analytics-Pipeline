import boto3
import pandas as pd
from kafka import KafkaProducer
import json
import time
from datetime import datetime,date
from io import BytesIO
import argparse
import sys
import logging

stream_server = [
    'localhost:9092'
    ]

def setup(stream_server):

    s3_client = boto3.client('s3')

    producer = KafkaProducer(
        bootstrap_servers = stream_server,
        value_serializer = lambda v: json.dumps(v, default=str).encode('utf-8'),
        acks = 'all',
        retries=3,
        compression_type = 'gzip'
    )

    return s3_client, producer

def get_latest_file_from_s3(s3_client,bucket):

    buffer = BytesIO()
    today_date = date.today()
    object_key = f'retail_data/transactions/transactions_{str(today_date)}.parquet'
    transaction_file = s3_client.download_fileobj(
        bucket,
        object_key,
        buffer
    )
    buffer.seek(0)

    df = pd.read_parquet(buffer)
    return df

if __name__=='__main__':

    s3_client, producer = setup(stream_server)

    df = get_latest_file_from_s3(s3_client, 'my-retail-2026-analytics-5805')

    for idx, row in df.iterrows():
        print(row)
    
    