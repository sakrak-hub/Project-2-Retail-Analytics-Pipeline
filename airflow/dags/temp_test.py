from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add scripts to path for imports
sys.path.insert(0, '/opt/airflow/scripts')

from utils.helpers import log_execution_time
from utils.database import upload_to_warehouse

default_args = {
    'owner': 'sho',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'retail_transaction_pipeline',
    default_args=default_args,
    description='Generate and process retail transactions',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['retail', 'transactions'],
) as dag:
    
    # Generate transaction data
    generate_transactions = BashOperator(
        task_id='generate_transactions',
        bash_command='python /opt/airflow/scripts/data_generators/generate_transactions.py --date {{ ds }}'
    )
    
    # Clean and validate data
    clean_data = BashOperator(
        task_id='clean_data',
        bash_command='python /opt/airflow/scripts/transformations/clean_data.py --input /tmp/raw_transactions_{{ ds }}.csv --output /tmp/clean_transactions_{{ ds }}.csv'
    )
    
    # Upload to warehouse using imported function
    upload_data = PythonOperator(
        task_id='upload_to_warehouse',
        python_callable=upload_to_warehouse,
        op_kwargs={
            'file_path': '/tmp/clean_transactions_{{ ds }}.csv',
            'table_name': 'staging.transactions'
        }
    )
    
    generate_transactions >> clean_data >> upload_data