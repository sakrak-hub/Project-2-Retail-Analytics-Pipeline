import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json
import threading
import time
from collections import deque
import logging

KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_TOPIC = 'daily-retail-transactions'
KAFKA_GROUP = 'streamlit-consumer'
UPDATE_INTERVAL = 2 
UPDATE_EVERY_N_ROWS = 1000  

def kafka_consumer_thread(transactions_list, stop_event):
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id=KAFKA_GROUP,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    
    try:
        for message in consumer:
            if stop_event.is_set():
                break

            event = message.value
            transactions_list.append(event)
    
    except Exception as e:
        logging.error(f"Kafka consumer error: {e}")
    
    finally:
        consumer.close()

def init_session_state():
    if 'transactions' not in st.session_state:
        st.session_state.transactions = deque(maxlen=50000)  
    
    if 'consumer_thread' not in st.session_state:
        st.session_state.consumer_thread = None
    
    if 'stop_event' not in st.session_state:
        st.session_state.stop_event = threading.Event()
    
    if 'consumer_running' not in st.session_state:
        st.session_state.consumer_running = False
    
    if 'last_display_count' not in st.session_state:
        st.session_state.last_display_count = 0  


def start_consumer():
    if not st.session_state.consumer_running:
        st.session_state.stop_event.clear()
        st.session_state.consumer_thread = threading.Thread(
            target=kafka_consumer_thread,
            args=(st.session_state.transactions, st.session_state.stop_event),
            daemon=True
        )
        st.session_state.consumer_thread.start()
        st.session_state.consumer_running = True
        st.success("✅ Kafka consumer started!")


def stop_consumer():
    if st.session_state.consumer_running:
        st.session_state.stop_event.set()
        st.session_state.consumer_running = False
        st.warning("⚠️ Kafka consumer stopped")


def clean_df(df):
    """Clean and prepare DataFrame"""
    store_df = pd.read_parquet('/mnt/d/Projects/Project-2-Retail-Analytics-Pipeline/tmp/master_data/stores.parquet')
    
    
    df = df[~df['transaction_id'].str.contains('DUP')]
    
    
    df = df.drop(columns=['store_name'], errors='ignore')
    df = pd.merge(df, store_df[['store_id','store_name']], on='store_id', how='left')
    
    
    df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S').dt.time
    df.sort_values(by='time', ascending=True, na_position='last', inplace=True)
    
    return df


def aggregate_to_transaction_level(df):

    transaction_df = df.groupby('transaction_id').agg({
        'store_id': 'first',
        'store_name': 'first',
        'date': 'first',
        'time': 'first',
        'datetime': 'first',
        'customer_id': 'first',
        'payment_method': 'first',
        'total_amount': 'first',  
        'tax_amount': 'first',
        'subtotal': 'first',
        'items_count': 'first',
        'line_total': 'sum',  
        'quantity': 'sum',  
        'status': 'first'
    }).reset_index()
    
    return transaction_df
    

def display_store_metrics_grid(df):
    """Display store metrics with proper aggregation"""
    if df.empty:
        st.info("No data yet. Waiting for transactions...")
        return
    
    
    df = clean_df(df)

    transaction_df = aggregate_to_transaction_level(df)

    unique_stores = transaction_df["store_name"].unique()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "Total Transactions", 
            f"{len(transaction_df):,}"  
        )
    with col2:
        st.metric(
            "Total Revenue", 
            f"${transaction_df['total_amount'].sum():,.2f}"  
        )
    with col3:
        st.metric(
            "Active Stores", 
            f"{len(unique_stores)}"  
        )
    with col4:
        st.metric(
            "Avg Transaction", 
            f"${transaction_df['total_amount'].mean():,.2f}"
        )
    
    st.divider()
    
    
    for i in range(0, len(unique_stores), 4):
        row_stores = unique_stores[i:i+4]
        row_cols = st.columns(len(row_stores))
        
        for col_idx, store_name in enumerate(row_stores):
            
            filtered = transaction_df[transaction_df["store_name"] == store_name]
            
            with row_cols[col_idx]:
                st.subheader(store_name)
                st.metric(
                    "💰 Total Sales", 
                    f"${filtered['total_amount'].sum():,.2f}"  
                )
                st.metric(
                    "📊 Transactions", 
                    f"{len(filtered):,}"  
                )
                st.metric(
                    "🛒 Avg Basket", 
                    f"${filtered['total_amount'].mean():,.2f}"
                )
                st.metric(
                    "📦 Avg Items", 
                    f"{filtered['items_count'].mean():.1f}"
                )


def should_update_display():
    """
    Check if we should update the display
    
    Only update every 1000 new rows to reduce overhead
    """
    current_count = len(st.session_state.transactions)
    last_count = st.session_state.last_display_count
    
    
    if current_count - last_count >= UPDATE_EVERY_N_ROWS:
        st.session_state.last_display_count = current_count
        return True
    
    
    if last_count == 0 and current_count > 0:
        st.session_state.last_display_count = current_count
        return True
    
    return False


def main():
    st.set_page_config(page_title="Real-time Store Dashboard", layout="wide")
    
    init_session_state()

    st.title("🏪 Real-time Store Performance Dashboard")

    
    col1, col2, col3, col4 = st.columns([1, 1, 2, 2])
    with col1:
        if st.button("▶️ Start Consumer", disabled=st.session_state.consumer_running):
            start_consumer()
    with col2:
        if st.button("⏸️ Stop Consumer", disabled=not st.session_state.consumer_running):
            stop_consumer()
    with col3:
        st.write(f"Status: {'🟢 Running' if st.session_state.consumer_running else '🔴 Stopped'}")
    with col4:
        st.write(f"Rows received: {len(st.session_state.transactions):,}")
    
    st.divider()
    
    
    if len(st.session_state.transactions) > 0:
        
        if should_update_display():
            df = pd.DataFrame(list(st.session_state.transactions))
            display_store_metrics_grid(df)
            st.caption(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')} | Showing {len(st.session_state.transactions):,} rows")
        else:
            
            st.info(f"📊 Accumulating data... Will update display every {UPDATE_EVERY_N_ROWS:,} rows. Current: {len(st.session_state.transactions):,}")
    else:
        st.info("👆 Click 'Start Consumer' to begin receiving transactions")

    
    if st.session_state.consumer_running:
        time.sleep(UPDATE_INTERVAL)
        st.rerun()


if __name__ == '__main__':
    main()