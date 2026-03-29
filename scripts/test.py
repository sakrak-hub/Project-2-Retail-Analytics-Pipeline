import pandas as pd
import streamlit
import matplotlib.pyplot as plt
from kafka import KafkaConsumer

kafka_consumer = KafkaConsumer(
    topic,
    bootstrap_servers = ['localhost:9092'],
    value_deserializer = lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='s3_parquet'
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

if __name__=='__main__':
    try:
        transactions_list = []
        for message in kafka_consumer:
            event = message.value
            transactions_list.append(event)
            round+=1
            
            