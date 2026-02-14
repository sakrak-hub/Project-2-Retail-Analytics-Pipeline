from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging
import duckdb
import subprocess

with DAG(
    'retail_analytics_dbt_duckdb_staging_modelling',
    default_args={
        'owner': 'Sakkaravarthi',
        'depends_on_past': False,
        'email': ['sakra_k@outlook.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='Production retail analytics pipeline with quality gates',
    schedule='0 * * * *', 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['retail', 'dbt', 'quality-gates', 'incremental']
) as dag:

    silver_layer_start = EmptyOperator(task_id='silver_layer_start')

    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command='cd /opt/airflow/dbt && dbt run --select staging --profiles-dir /opt/airflow/dbt'
    )

    silver_layer_start >> dbt_run_silver