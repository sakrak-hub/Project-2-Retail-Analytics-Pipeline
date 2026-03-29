from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import io

s3_client = boto3.client('s3')
bucket_name = 'my-retail-2026-analytics-5805'
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

def convert_dtypes(df):
    new_df = df.copy()
    
    new_df['date'] = pd.to_datetime(df['date'], errors='coerce')
    new_df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    
    numeric_cols = ['subtotal', 'tax_amount', 'total_amount', 
                    'items_count', 'loyalty_points_earned', 
                    'quantity', 'unit_price', 'discount_percent', 
                    'line_total']
    for col in numeric_cols:
        if col in new_df.columns:
            new_df[col] = pd.to_numeric(new_df[col], errors='coerce')
    
    string_cols = ['transaction_id', 'customer_id', 'store_id', 
                   'cashier_id', 'product_id', 'store_name', 
                   'payment_method', 'status', 'product_name', 'category']
    for col in string_cols:
        if col in new_df.columns:
            new_df[col] = new_df[col].astype(str).replace('nan', None)
    
    return new_df

def upload_df_to_s3(df,bucket_name,object_key):
    df = convert_dtypes(df)
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine='pyarrow',compression='snappy')
    buffer.seek(0)
    s3_client.upload_fileobj(
                Fileobj=buffer,
                Bucket=bucket_name,
                Key=object_key
            )

def update_or_create_s3_object(bucket_name, object_key, df):
    s3 = boto3.client('s3')
    try:
        df = convert_dtypes(df)
        parquet_df = pd.read_parquet(f's3://{bucket_name}/{object_key}')
        parquet_df = pd.concat([parquet_df,df],ignore_index=True)
        upload_df_to_s3(parquet_df,bucket_name,object_key)
        print(f"Uploaded {len(parquet_df)} rows!")
    except (FileNotFoundError, OSError) as e:
        upload_df_to_s3(df,bucket_name,object_key)

if __name__=='__main__':
    try:
        transactions_list = []
        for message in kafka_consumer:
            event = message.value
            transactions_list.append(event)
            if len(transactions_list)==1000:
                object_key = f"retail_data/transactions/transactions_{event.get('date',str(datetime.now().date()))}.parquet"
                print(f"Uploading to {object_key}")
                append_df = pd.DataFrame(transactions_list)
                update_or_create_s3_object(bucket_name,object_key,append_df)
                print(f"{object_key} upload done!")
                transactions_list = []
                
    except KeyboardInterrupt:
        kafka_consumer.close()
    finally:
        kafka_consumer.close()