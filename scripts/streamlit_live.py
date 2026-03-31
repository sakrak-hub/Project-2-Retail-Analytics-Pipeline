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
        st.error(f"Kafka consumer error: {e}")
    
    finally:
        consumer.close()

def init_session_state():
    if 'transactions' not in st.session_state:
        st.session_state.transactions = deque(maxlen=10000)
    
    if 'consumer_thread' not in st.session_state:
        st.session_state.consumer_thread = None
    
    if 'stop_event' not in st.session_state:
        st.session_state.stop_event = threading.Event()
    
    if 'consumer_running' not in st.session_state:
        st.session_state.consumer_running = False


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
    store_df = pd.read_parquet('/mnt/d/Projects/Project-2-Retail-Analytics-Pipeline/tmp/master_data/stores.parquet')
    df = df[~df['transaction_id'].str.contains('DUP')]
    df= df.drop(columns=['store_name'])
    df = pd.merge(df, store_df[['store_id','store_name']], on='store_id', how='left')
    df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S').dt.time
    df.sort_values(by='time', ascending=True, na_position='last',inplace=True)
    return df
    

def display_store_metrics_grid(df):
    if df.empty:
        st.info("No data yet. Waiting for transactions...")
        return
    
    df = clean_df(df)
    unique_stores = df["store_name"].unique()
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Transactions", f"{len(df):,}")
    with col2:
        st.metric("Total Revenue", f"${df['line_total'].sum():,.0f}")
    with col3:
        st.metric("Active Stores", f"{len(unique_stores)}")
    
    st.divider()
    
    for i in range(0, len(unique_stores), 4):
        row_stores = unique_stores[i:i+4]
        row_cols = st.columns(len(row_stores))
        
        for col_idx, store_name in enumerate(row_stores):
            filtered = df[df["store_name"] == store_name]
            
            with row_cols[col_idx]:
                st.subheader(store_name)
                st.metric(
                    "💰 Total Sales", 
                    f"${filtered['line_total'].sum():,.0f}"
                )
                st.metric(
                    "📊 Transactions", 
                    f"{filtered['transaction_id'].nunique():,}"
                )

def main():
    st.set_page_config(page_title="Real-time Store Dashboard", layout="wide")
    
    init_session_state()

    st.title("🏪 Real-time Store Performance Dashboard")

    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        if st.button("▶️ Start Consumer", disabled=st.session_state.consumer_running):
            start_consumer()
    with col2:
        if st.button("⏸️ Stop Consumer", disabled=not st.session_state.consumer_running):
            stop_consumer()
    with col3:
        st.write(f"Status: {'🟢 Running' if st.session_state.consumer_running else '🔴 Stopped'}")
    
    st.divider()
    
    if len(st.session_state.transactions) > 0:
        df = pd.DataFrame(list(st.session_state.transactions))

        display_store_metrics_grid(df)

        st.caption(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        st.info("👆 Click 'Start Consumer' to begin receiving transactions")

    if st.session_state.consumer_running:
        time.sleep(UPDATE_INTERVAL)
        st.rerun()

if __name__ == '__main__':
    main()