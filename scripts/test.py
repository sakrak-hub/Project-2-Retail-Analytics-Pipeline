import pandas as pd
from kafka import KafkaConsumer
import json

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

def clean_df(df):
    store_df = pd.read_parquet('/mnt/d/Projects/Project-2-Retail-Analytics-Pipeline/tmp/master_data/stores.parquet')
    df = df[~df['transaction_id'].str.contains('DUP')]
    df= df.drop(columns=['store_name'])
    df = pd.merge(df, store_df[['store_id','store_name']], on='store_id', how='left')
    df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S').dt.time
    df.sort_values(by='time', ascending=True, na_position='last',inplace=True)
    return df

transactions_list = []
for message in kafka_consumer:
    event = message.value
    transactions_list.append(event)
    print(f"List length - {len(transactions_list)}")
    if (len(transactions_list)%1000)==0 and len(transactions_list)>0:
        event_df = pd.DataFrame(transactions_list)
        event_df = clean_df(event_df)
        unique_stores = event_df["store_name"].unique()
        for col_idx, store_name in enumerate(unique_stores):
            filtered = event_df[event_df["store_name"] == store_name]
            print(f"{store_name} number of transactions - {filtered['transaction_id'].nunique()}")
            print(f"{store_name} total revenue - ${filtered['line_total'].sum():.2f}")
