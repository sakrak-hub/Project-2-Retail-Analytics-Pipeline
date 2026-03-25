from data_generator_2 import RetailDataGenerator
from kafka import KafkaProducer
import boto3
import pandas as pd
import json
from datetime import datetime, time
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

    records = []
    for transaction in transactions:
        base_txn = {k:v for k,v in transaction.items() if k !='items'}
        
        if transaction.get('items'):
            for item in transaction['items']:
                record = {**base_txn, **item}
                records.append(record)
        else:
            records.append(base_txn)
        
    return records

def stream_transaction(df):

    df.sort_values(by='time')
    date_today = datetime.now()

    while date_today.time() < time(hour=22):
        pass



if __name__=='__main__':

    transaction_date = datetime.now()

    retail_generator = RetailDataGenerator(folder_path='/mnt/d/Projects/Project-2-Retail-Analytics-Pipeline/tmp/master_data')

    data = retail_generator.generate_daily_transactions(transaction_date)

    transactions_records = flatten_transactions(data)
    for transaction in transactions_records:
        if transaction['time'] is None:
            transaction['time']='23:59:59'

    transactions_records = sorted(transactions_records, key=lambda item:(datetime.strptime(item['time'],"%H:%M:%S")))
    time_offset = time(hour=8)
    while (datetime.now().time()<time(hour=22)):
        filtered_list = [transaction for transaction in transactions 
        if datetime.strptime(transaction['time'],'%H:%M:%S')>=time_offset
        and datetime.strptime(transaction['time'],'%H:%M:%S')>=datetime.now().time()]

        for transaction in filtered_list:
            print(transaction)
            time_offset=datetime.strptime(transaction['time'],'%H:%M:%S')
    # transactions_df = pd.DataFrame(transactions_records)
    # time_offset = time(hour=8)
    # time_now = datetime.now().time()
    # transactions_df['time'] = pd.to_datetime(transactions_df['time']).dt.time
    

    # while (datetime.now().time()<time(hour=22)):
    #     filtered_df = transactions_df[transactions_df['time'].between(time_offset,datetime.now().time())].sort_values(by='time')
    #     if len(filtered_df)!=0:
    #         for index, transaction in filtered_df.iterrows():
    #             print(transaction)
    #             time_offset = transaction['time']
    #     transactions_df = transactions_df[~transactions_df.index.isin(filtered_df.index)]
    #     if len(df)==0:
    #         break

