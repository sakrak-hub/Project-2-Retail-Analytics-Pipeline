import boto3
import pandas as pd
from kafka import KafkaProducer
import json
import time
from datetime import datetime
from io import BytesIO
import argparse
import sys
import logging

logger = logging.getLogger(__name__)

class S3ToKafkaStreamer:
    def __init__(self, kafka_bootstrap='localhost:9092'):
        """Initialize S3 and Kafka connections"""
        print("🔧 Initializing connections...")
        
        self.s3_client = boto3.client('s3')
        
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_bootstrap],
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            acks='all',
            retries=3,
            compression_type='gzip'
        )
        
        print("✅ Connected to S3 and Kafka")
        
    def list_parquet_files(self, bucket, prefix):
        """List all parquet files in S3 bucket/prefix"""
        print(f"\n📂 Listing files in s3://{bucket}/{prefix}")
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            
            files = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.parquet'):
                            files.append({
                                'key': obj['Key'],
                                'size': obj['Size'],
                                'modified': obj['LastModified']
                            })
            
            files = sorted(files, key=lambda x: x['key'])
            
            print(f"✅ Found {len(files)} parquet files")
            
            # Show file list
            total_size = sum(f['size'] for f in files)
            print(f"   Total size: {total_size / (1024**2):.2f} MB")
            
            return files
            
        except Exception as e:
            print(f"❌ Error listing S3 files: {e}")
            raise
    
    def read_parquet_from_s3(self, bucket, key):
        """Read parquet file from S3 into pandas DataFrame"""
        print(f"\n📖 Reading s3://{bucket}/{key}")
        
        try:
            # Download parquet file to memory
            buffer = BytesIO()
            self.s3_client.download_fileobj(bucket, key, buffer)
            buffer.seek(0)
            
            # Read with pandas
            df = pd.read_parquet(buffer)
            
            print(f"   ✅ Loaded {len(df):,} records")
            print(f"   Columns: {', '.join(df.columns.tolist())}")
            
            return df
            
        except Exception as e:
            print(f"   ❌ Error reading parquet: {e}")
            raise
    
    def prepare_event(self, row, record_num, total_records):
        """Convert DataFrame row to Kafka event"""
        event = row.to_dict()
        
        # Add streaming metadata
        event['_streaming_metadata'] = {
            'source': 'S3',
            'streamed_at': datetime.now().isoformat(),
            'record_number': record_num,
            'total_records': total_records
        }
        
        # Convert pandas types to JSON-serializable types
        for key, value in event.items():
            if pd.isna(value):
                event[key] = None
            elif isinstance(value, (pd.Timestamp, datetime)):
                event[key] = value.isoformat()
            elif isinstance(value, (pd.Int64Dtype, pd.Int32Dtype)):
                event[key] = int(value)
        
        return event
    
    def stream_dataframe(self, df, topic, delay_seconds=0.1, batch_size=100):
        """Stream DataFrame rows to Kafka with delay"""
        total_records = len(df)
        
        print(f"\n🚀 Streaming {total_records:,} records to topic '{topic}'")
        print(f"   Delay: {delay_seconds}s between events")
        print(f"   Rate: {1/delay_seconds if delay_seconds > 0 else 'unlimited'} events/sec")
        print()
        
        sent_count = 0
        start_time = time.time()
        
        for idx, row in df.iterrows():
            # Prepare event
            event = self.prepare_event(row, idx + 1, total_records)
            
            # Send to Kafka
            try:
                future = self.producer.send(topic, value=event)
                
                # Don't wait for every message (performance optimization)
                if (idx + 1) % batch_size == 0:
                    future.get(timeout=10)  # Wait for batch
                    self.producer.flush()
                
                sent_count += 1
                
            except Exception as e:
                print(f"\n❌ Error sending record {idx + 1}: {e}")
                continue
            
            # Progress indicator
            if (idx + 1) % batch_size == 0 or (idx + 1) == total_records:
                elapsed = time.time() - start_time
                rate = sent_count / elapsed if elapsed > 0 else 0
                progress = (idx + 1) / total_records * 100
                
                print(f"   📊 Sent {idx + 1:,} / {total_records:,} records "
                      f"({progress:.1f}%) | Rate: {rate:.1f} events/sec")
            
            # Simulate real-time with delay
            if delay_seconds > 0:
                time.sleep(delay_seconds)
        
        # Final flush
        self.producer.flush()
        
        elapsed = time.time() - start_time
        final_rate = sent_count / elapsed if elapsed > 0 else 0
        
        print(f"\n✅ Completed streaming {sent_count:,} records")
        print(f"   Duration: {elapsed:.2f} seconds")
        print(f"   Average rate: {final_rate:.1f} events/sec")
    
    def stream_all_files(self, bucket, prefix, topic, delay_seconds=0.1, 
                         max_files=None):
        """Stream all parquet files from S3 to Kafka"""
        files = self.list_parquet_files(bucket, prefix)
        
        if not files:
            print("❌ No parquet files found!")
            return
        
        # Limit files if requested
        if max_files:
            files = files[:max_files]
            print(f"⚠️  Limited to first {max_files} files")
        
        print(f"\n🚀 Starting stream from {len(files)} files...")
        print(f"   Source: s3://{bucket}/{prefix}")
        print(f"   Target: Kafka topic '{topic}'")
        print(f"   Delay: {delay_seconds}s between events")
        print("=" * 60)
        
        total_streamed = 0
        total_start = time.time()
        
        for file_idx, file_info in enumerate(files, 1):
            file_key = file_info['key']
            
            print(f"\n📁 File {file_idx}/{len(files)}: {file_key}")
            print(f"   Size: {file_info['size'] / 1024:.2f} KB")
            
            # Read parquet file
            df = self.read_parquet_from_s3(bucket, file_key)
            
            # Stream to Kafka
            self.stream_dataframe(df, topic, delay_seconds)
            
            total_streamed += len(df)
            print(f"   📊 Total streamed so far: {total_streamed:,} records")
        
        total_elapsed = time.time() - total_start
        overall_rate = total_streamed / total_elapsed if total_elapsed > 0 else 0
        
        print("\n" + "=" * 60)
        print(f"🎉 COMPLETE!")
        print(f"   Total records: {total_streamed:,}")
        print(f"   Total duration: {total_elapsed:.2f} seconds")
        print(f"   Overall rate: {overall_rate:.1f} events/sec")
        print("=" * 60)
        
    def close(self):
        """Close Kafka producer"""
        print("\n🔒 Closing Kafka producer...")
        self.producer.close()
        print("✅ Producer closed")

def main():
    parser = argparse.ArgumentParser(
        description='Stream parquet files from S3 to Kafka',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Stream all files with 100ms delay
  python s3_to_kafka.py --bucket my-bucket --prefix data/ --topic events --delay 0.1
  
  # Stream as fast as possible
  python s3_to_kafka.py --bucket my-bucket --prefix data/ --topic events --no-delay
  
  # Stream only first 5 files
  python s3_to_kafka.py --bucket my-bucket --prefix data/ --topic events --max-files 5
        """
    )
    
    parser.add_argument('--bucket', required=True, 
                        help='S3 bucket name', default='my-retail-2026-analytics-5805')
    parser.add_argument('--prefix', required=True, 
                        help='S3 prefix/folder path', default='retail_data/transactions/')
    parser.add_argument('--topic', required=True, 
                        help='Kafka topic name')
    parser.add_argument('--kafka', default='localhost:9092', 
                        help='Kafka bootstrap server (default: localhost:9092)')
    parser.add_argument('--delay', type=float, default=0.1, 
                        help='Delay between events in seconds (default: 0.1)')
    parser.add_argument('--no-delay', action='store_true', 
                        help='Stream as fast as possible (no delay)')
    parser.add_argument('--max-files', type=int, 
                        help='Maximum number of files to process')
    
    args = parser.parse_args()
    
    delay = 0 if args.no_delay else args.delay
    
    print("=" * 60)
    print("S3 PARQUET → KAFKA STREAMER")
    print("=" * 60)
    print(f"Source: s3://{args.bucket}/{args.prefix}")
    print(f"Target: Kafka topic '{args.topic}'")
    print(f"Kafka:  {args.kafka}")
    print(f"Delay:  {delay}s between events")
    if args.max_files:
        print(f"Limit:  {args.max_files} files")
    print("=" * 60)
    
    streamer = S3ToKafkaStreamer(kafka_bootstrap=args.kafka)
    
    try:
        streamer.stream_all_files(
            bucket=args.bucket,
            prefix=args.prefix,
            topic=args.topic,
            delay_seconds=delay,
            max_files=args.max_files
        )
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user (Ctrl+C)")
        print("   Flushing remaining messages...")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        streamer.close()
        print("\n👋 Streamer shutdown complete")
 
 
if __name__ == "__main__":
    main()