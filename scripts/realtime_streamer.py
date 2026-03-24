#!/usr/bin/env python3
"""
Real-Time Transaction Streamer with Realistic Delays

Generates transactions continuously with realistic timing:
- Transactions per second: configurable (default: 10)
- Timestamp delays: 0-300 seconds (0-5 minutes)
- Distribution: 60% fresh (0-30s), 25% normal (30-120s), 15% slow (2-5min)

Usage:
    python realtime_streamer.py \
        --rate 10 \
        --max-delay 300 \
        --duration 600
"""

import sys
sys.path.append('/opt/airflow')

from data_generator_2 import RetailDataGenerator
from kafka import KafkaProducer
import boto3
import json
import random
import time
from datetime import datetime, timedelta
from io import BytesIO
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RealTimeStreamer:
    """
    Real-time transaction streaming with realistic delays
    
    Simulates a live POS system:
    - Continuous generation at specified rate
    - Recent timestamps (0-5 minutes ago)
    - Realistic delay distribution
    - Routes to Kafka (today) or S3 (historical/NULL)
    """
    
    def __init__(
        self,
        generator,
        s3_bucket=None,
        s3_prefix='raw/transactions/',
        kafka_topic='pos-transactions',
        kafka_bootstrap='localhost:9092',
        transactions_per_second=10,
        max_delay_seconds=300
    ):
        self.generator = generator
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.rstrip('/') + '/' if s3_prefix else 'raw/transactions/'
        self.kafka_topic = kafka_topic
        self.rate = transactions_per_second
        self.max_delay = max_delay_seconds
        
        # Delay distribution (realistic)
        # 60% fresh (0-30s), 25% normal (30-120s), 15% slow (2-5min)
        self.delay_ranges = [
            (0, 30, 0.60),              # 60%: 0-30 seconds
            (30, 120, 0.25),            # 25%: 30 seconds - 2 minutes
            (120, max_delay_seconds, 0.15)  # 15%: 2-5 minutes
        ]
        
        # Initialize Kafka
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=[kafka_bootstrap],
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            acks='all',
            retries=3,
            compression_type='gzip'
        )
        
        # Initialize S3 if bucket provided
        self.s3_client = boto3.client('s3') if s3_bucket else None
        self.s3_buffer = []
        
        # Statistics
        self.stats = {
            'total': 0,
            'kafka': 0,
            's3': 0,
            'delays': []  # Track delays for analysis
        }
        
        logger.info("✅ Real-time streamer initialized")
        logger.info(f"   Rate: {transactions_per_second} txn/sec")
        logger.info(f"   Max delay: {max_delay_seconds}s ({max_delay_seconds/60:.1f} minutes)")
        logger.info(f"   TODAY → Kafka: {kafka_topic}")
        if s3_bucket:
            logger.info(f"   HISTORICAL → S3: s3://{s3_bucket}/{s3_prefix}")
    
    def _generate_realistic_delay(self):
        """
        Generate delay in seconds based on distribution
        
        Returns:
            delay_seconds (float): 0 to max_delay
        """
        rand = random.random()
        cumulative = 0
        
        for min_sec, max_sec, probability in self.delay_ranges:
            cumulative += probability
            if rand <= cumulative:
                return random.uniform(min_sec, max_sec)
        
        # Fallback
        return random.uniform(0, self.max_delay)
    
    def _generate_transaction_with_timestamp(self):
        """
        Generate single transaction with realistic timestamp
        
        Uses generator's logic but overrides timestamp to be recent
        """
        # Use generator to create a single transaction
        # We'll call the internal method and extract one transaction
        date = datetime.now()
        transactions = self.generator.generate_daily_transactions(date)
        
        if not transactions:
            return None
        
        # Take first transaction
        txn = transactions[0]
        
        # Calculate realistic timestamp
        delay_seconds = self._generate_realistic_delay()
        realistic_timestamp = datetime.now() - timedelta(seconds=delay_seconds)
        
        # Override timestamp
        txn['datetime'] = realistic_timestamp
        txn['date'] = realistic_timestamp.date()
        txn['time'] = realistic_timestamp.time()
        
        # Add metadata
        txn['_delay_seconds'] = delay_seconds
        txn['_generated_at'] = datetime.now()
        
        return txn, delay_seconds
    
    def _flatten_transaction(self, txn):
        """Flatten nested transaction to records"""
        base_txn = {k: v for k, v in txn.items() if k != 'items'}
        
        records = []
        if txn.get('items'):
            for item in txn['items']:
                record = {**base_txn, **item}
                records.append(record)
        else:
            records.append(base_txn)
        
        return records
    
    def _route_record(self, record):
        """Route single record to Kafka or S3"""
        txn_datetime = record.get('datetime')
        
        # NULL timestamp → S3
        if txn_datetime is None or pd.isna(txn_datetime):
            self._route_to_s3(record)
            return
        
        # Check date
        if isinstance(txn_datetime, str):
            txn_datetime = pd.to_datetime(txn_datetime)
        
        txn_date = txn_datetime.date()
        today = datetime.now().date()
        
        # TODAY → Kafka, else → S3
        if txn_date == today:
            self._route_to_kafka(record)
        else:
            self._route_to_s3(record)
    
    def _route_to_kafka(self, record):
        """Send to Kafka"""
        try:
            # Prepare for JSON serialization
            record_copy = record.copy()
            
            # Convert datetime objects to strings
            for key in ['datetime', 'date', 'time', '_generated_at']:
                if key in record_copy and record_copy[key] is not None:
                    if hasattr(record_copy[key], 'isoformat'):
                        record_copy[key] = record_copy[key].isoformat()
                    else:
                        record_copy[key] = str(record_copy[key])
            
            self.kafka_producer.send(self.kafka_topic, value=record_copy)
            self.stats['kafka'] += 1
            
        except Exception as e:
            logger.error(f"Kafka send failed: {e}")
    
    def _route_to_s3(self, record):
        """Buffer for S3"""
        if self.s3_client:
            self.s3_buffer.append(record)
            self.stats['s3'] += 1
            
            # Flush if buffer full
            if len(self.s3_buffer) >= 100:
                self._flush_s3()
    
    def _flush_s3(self):
        """Write S3 buffer to parquet"""
        if not self.s3_buffer or not self.s3_client:
            return
        
        try:
            df = pd.DataFrame(self.s3_buffer)
            
            # Handle datetime columns
            for col in ['datetime', 'date', '_generated_at']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"realtime_{timestamp}.parquet"
            s3_key = f"{self.s3_prefix}{filename}"
            
            # Write
            buffer_io = BytesIO()
            df.to_parquet(buffer_io, engine='pyarrow', compression='snappy', index=False)
            buffer_io.seek(0)
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=buffer_io.getvalue()
            )
            
            logger.info(f"✅ S3: Wrote {len(self.s3_buffer)} records to {filename}")
            self.s3_buffer = []
            
        except Exception as e:
            logger.error(f"S3 write failed: {e}")
    
    def run(self, duration_seconds=None):
        """
        Run continuous streaming
        
        Args:
            duration_seconds: How long to run (None = forever)
        """
        logger.info("\n🚀 Starting real-time streaming...")
        logger.info(f"   Rate: {self.rate} transactions/second")
        logger.info(f"   Duration: {'Forever' if not duration_seconds else f'{duration_seconds}s'}")
        logger.info("")
        
        start_time = time.time()
        
        try:
            while True:
                # Generate transaction with realistic timestamp
                result = self._generate_transaction_with_timestamp()
                
                if result is None:
                    continue
                
                txn, delay = result
                
                # Flatten and route
                records = self._flatten_transaction(txn)
                for record in records:
                    self._route_record(record)
                
                self.stats['total'] += len(records)
                self.stats['delays'].append(delay)
                
                # Logging every 10 transactions
                if self.stats['total'] % 10 == 0:
                    avg_delay = sum(self.stats['delays'][-10:]) / 10
                    timestamp_str = txn['datetime'].strftime('%H:%M:%S') if txn['datetime'] else 'NULL'
                    
                    logger.info(
                        f"📊 Sent: {self.stats['total']:4d} | "
                        f"Kafka: {self.stats['kafka']:4d} | "
                        f"S3: {self.stats['s3']:4d} | "
                        f"Delay: {delay:5.1f}s | "
                        f"Avg: {avg_delay:5.1f}s | "
                        f"Timestamp: {timestamp_str}"
                    )
                
                # Rate limiting
                time.sleep(1.0 / self.rate)
                
                # Check duration
                if duration_seconds:
                    elapsed = time.time() - start_time
                    if elapsed >= duration_seconds:
                        logger.info(f"\n⏱️  Duration reached: {duration_seconds}s")
                        break
        
        except KeyboardInterrupt:
            logger.info("\n⚠️  Stopped by user")
        
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Cleanup and print final stats"""
        logger.info("\n🔄 Cleaning up...")
        
        # Flush buffers
        if self.s3_buffer:
            self._flush_s3()
        
        self.kafka_producer.flush()
        self.kafka_producer.close()
        
        # Print final statistics
        elapsed = time.time() - start_time if 'start_time' in locals() else 0
        
        print("\n" + "=" * 70)
        print("STREAMING STATISTICS")
        print("=" * 70)
        print(f"Total records:     {self.stats['total']:,}")
        print(f"├─ Kafka (today):  {self.stats['kafka']:,} ({self.stats['kafka']/max(1, self.stats['total'])*100:.1f}%)")
        print(f"└─ S3 (other):     {self.stats['s3']:,} ({self.stats['s3']/max(1, self.stats['total'])*100:.1f}%)")
        
        if self.stats['delays']:
            print(f"\nDelay Statistics:")
            print(f"├─ Min:  {min(self.stats['delays']):.1f}s")
            print(f"├─ Max:  {max(self.stats['delays']):.1f}s")
            print(f"├─ Avg:  {sum(self.stats['delays'])/len(self.stats['delays']):.1f}s")
            print(f"└─ Med:  {sorted(self.stats['delays'])[len(self.stats['delays'])//2]:.1f}s")
        
        if elapsed > 0:
            print(f"\nDuration: {elapsed:.1f}s")
            print(f"Actual rate: {self.stats['total']/elapsed:.2f} txn/sec")
        
        print("=" * 70)
        print()
        
        logger.info("✅ Cleanup complete")

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Real-time transaction streaming with realistic delays',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Stream at 10 txn/sec for 5 minutes (300s)
  python realtime_streamer.py --rate 10 --duration 300
  
  # High volume: 100 txn/sec with 1-minute delays
  python realtime_streamer.py --rate 100 --max-delay 60
  
  # Continuous with S3 fallback
  python realtime_streamer.py --rate 10 --bucket my-bucket
        """
    )
    
    parser.add_argument('--rate', type=int, default=10, 
                       help='Transactions per second (default: 10)')
    parser.add_argument('--max-delay', type=int, default=300,
                       help='Maximum delay in seconds (default: 300 = 5 minutes)')
    parser.add_argument('--duration', type=int,
                       help='Run duration in seconds (default: forever)')
    parser.add_argument('--bucket', help='S3 bucket for historical data')
    parser.add_argument('--prefix', default='raw/transactions/',
                       help='S3 prefix (default: raw/transactions/)')
    parser.add_argument('--topic', default='pos-transactions',
                       help='Kafka topic (default: pos-transactions)')
    parser.add_argument('--kafka', default='localhost:9092',
                       help='Kafka bootstrap server (default: localhost:9092)')
    parser.add_argument('--seed', type=int, default=42,
                       help='Random seed for generator (default: 42)')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("REAL-TIME TRANSACTION STREAMER")
    print("=" * 70)
    print(f"Rate:      {args.rate} transactions/second")
    print(f"Max delay: {args.max_delay}s ({args.max_delay/60:.1f} minutes)")
    print(f"Kafka:     {args.kafka} → {args.topic}")
    if args.bucket:
        print(f"S3:        s3://{args.bucket}/{args.prefix}")
    print("=" * 70)
    print()
    
    # Initialize generator
    logger.info("Initializing RetailDataGenerator...")
    generator = RetailDataGenerator(seed=args.seed, add_noise=True)
    
    # Initialize streamer
    start_time = time.time()  # Define start_time in outer scope
    streamer = RealTimeStreamer(
        generator=generator,
        s3_bucket=args.bucket,
        s3_prefix=args.prefix,
        kafka_topic=args.topic,
        kafka_bootstrap=args.kafka,
        transactions_per_second=args.rate,
        max_delay_seconds=args.max_delay
    )
    
    # Run
    streamer.run(duration_seconds=args.duration)