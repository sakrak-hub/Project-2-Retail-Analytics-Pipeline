from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
import pandas as pd
import streamlit
import matplotlib.pyplot as plt
import boto3


kafka_bootstrap = ['localhost:9092']
topic='daily-retail-transactions'
group_id='s3_parquet'

kafka_consumer = KafkaConsumer(
    topic,
    bootstrap_servers = kafka_bootstrap,
    value_deserializer = lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id=group_id
)

if __name__=='__main__':
    try:
        transactions_list = []
        for message in kafka_consumer:
            event = message.value
            transactions_list.append(event)
            if len(transactions_list)>1000:
                
    except KeyboardInterrupt:
        kafka_consumer.close()
    finally:
        kafka_consumer.close()