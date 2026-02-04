from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta

default_args = {
    'owner': 'Sakkaravarthi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'retail_pipeline',
    default_args=default_args,
    description='Process retail data',
    schedule_interval='@daily',
    start_date=datetime(20261, 1),
    max_active_runs=1,
    catchup=False,
    tags=['retail'],
) as dag: