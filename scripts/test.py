"""
Streamlit + Kafka Consumer - Correct Implementation

Key concepts:
1. Kafka consumer runs in background thread
2. Data stored in Streamlit session state
3. UI refreshes periodically to show new data
4. No blocking loops in main Streamlit script
"""

import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json
import threading
import time
from collections import deque

# ========================================================================
# CONFIGURATION
# ========================================================================

KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_TOPIC = 'daily-retail-transactions'
KAFKA_GROUP = 'streamlit-consumer'
UPDATE_INTERVAL = 2  # Refresh every 2 seconds

# ========================================================================
# KAFKA CONSUMER THREAD (Runs in Background)
# ========================================================================

def kafka_consumer_thread(transactions_list, stop_event):
    """
    Runs Kafka consumer in background thread
    
    Args:
        transactions_list: Thread-safe deque to store transactions
        stop_event: Event to signal thread to stop
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id=KAFKA_GROUP,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    
    try:
        for message in consumer:
            # Check if we should stop
            if stop_event.is_set():
                break
            
            # Add transaction to shared list
            event = message.value
            transactions_list.append(event)
            
            # # Optional: Limit memory usage
            # if len(transactions_list) > 10000:
            #     transactions_list.popleft()  # Remove oldest
    
    except Exception as e:
        st.error(f"Kafka consumer error: {e}")
    
    finally:
        consumer.close()


# ========================================================================
# INITIALIZE SESSION STATE
# ========================================================================

def init_session_state():
    """Initialize Streamlit session state"""
    if 'transactions' not in st.session_state:
        st.session_state.transactions = deque(maxlen=10000)  # Thread-safe
    
    if 'consumer_thread' not in st.session_state:
        st.session_state.consumer_thread = None
    
    if 'stop_event' not in st.session_state:
        st.session_state.stop_event = threading.Event()
    
    if 'consumer_running' not in st.session_state:
        st.session_state.consumer_running = False


# ========================================================================
# START/STOP KAFKA CONSUMER
# ========================================================================

def start_consumer():
    """Start Kafka consumer in background thread"""
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
    """Stop Kafka consumer thread"""
    if st.session_state.consumer_running:
        st.session_state.stop_event.set()
        st.session_state.consumer_running = False
        st.warning("⚠️ Kafka consumer stopped")


# ========================================================================
# DISPLAY FUNCTIONS
# ========================================================================

def display_store_metrics_grid(df):
    """Display store metrics in grid layout"""
    if df.empty:
        st.info("No data yet. Waiting for transactions...")
        return
    
    unique_stores = df["store_name"].unique()
    
    # Display summary stats
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Transactions", f"{len(df):,}")
    with col2:
        st.metric("Total Revenue", f"${df['line_total'].sum():,.0f}")
    with col3:
        st.metric("Active Stores", f"{len(unique_stores)}")
    
    st.divider()
    
    # Store grid (4 columns)
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


# ========================================================================
# MAIN STREAMLIT APP
# ========================================================================

def main():
    st.set_page_config(page_title="Real-time Store Dashboard", layout="wide")
    
    # Initialize session state
    init_session_state()
    
    # Title and controls
    st.title("🏪 Real-time Store Performance Dashboard")
    
    # Control buttons
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
    
    # Display metrics
    if len(st.session_state.transactions) > 0:
        # Convert to DataFrame
        df = pd.DataFrame(list(st.session_state.transactions))
        
        # Display grid
        display_store_metrics_grid(df)
        
        # Show last update time
        st.caption(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        st.info("👆 Click 'Start Consumer' to begin receiving transactions")
    
    # Auto-refresh
    if st.session_state.consumer_running:
        time.sleep(UPDATE_INTERVAL)
        st.rerun()


# ========================================================================
# RUN APP
# ========================================================================

if __name__ == '__main__':
    main()