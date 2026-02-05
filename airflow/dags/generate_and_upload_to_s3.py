from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
from pathlib import Path

master_data_list = ["customers.parquet", "stores.parquet","products.parquet"]
s3_hook = S3Hook(aws_conn_id='aws_retailitics_s3')
bucket_name = "my-retail-2026-analytics-5805"

def check_file_exists(ds,**context):
    file_key = f"retail_data/transactions/transactions_{ds}.parquet"
    print(f"Checking for 'transactions_{ds}.parquet' in S3")

    if s3_hook.check_for_key(key=file_key, bucket_name=bucket_name):
        raise AirflowSkipException("Data found. Skipping task")
    else:
        print("Data not found. Generating data")

def upload_master_data_to_s3(files_list,**kwargs):
    
    for file in files_list:
        print(f"Upload {file} to S3")

        s3_hook.load_file(
            filename=f"/opt/airflow/master_data/{file}",
            key=f"retail_data/{file}",
            bucket_name=bucket_name,
            replace=True
        )

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
    schedule='@daily',
    start_date=datetime(2026, 1,1),
    max_active_runs=1,
    catchup=True
    tags=['retail', 'transactions'],
) as dag:

    file_checker = PythonOperator(
        task_id = 'verify_file_in_s3',
        python_callable = check_file_exists
    )

    generate_retail_data = BashOperator(
        task_id = 'generate_retail_data',
        trigger_rule="none_skipped",
        bash_command = 'python3 /opt/airflow/scripts/data_generator_2.py {{logical_date.year}} {{logical_date.month}} {{logical_date.day}}'
    )

    upload_master_data = PythonOperator(
        task_id = 'master_data_s3',
        python_callable = upload_master_data_to_s3,
        op_kwargs={'files_list': master_data_list}
    )

    upload_transactions_to_s3 = LocalFilesystemToS3Operator(
        task_id = 'upload_transactions',
        trigger_rule="none_skipped",
        filename = '/tmp/retail_data/transactions/transactions_{{ds}}.parquet',
        dest_key = 's3://my-retail-2026-analytics-5805/retail_data/transactions/transactions_{{ds}}.parquet',
        aws_conn_id='aws_retailitics_s3',
        replace = False,
    )

    file_checker >> generate_retail_data >> [upload_master_data, upload_transactions_to_s3] 