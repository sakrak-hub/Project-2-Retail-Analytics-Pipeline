from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from loguru import logger

logger = logging.getLogger(__name__)

def validate_data_quality(**context):
    """Validate raw data before transformation"""
    import duckdb
    
    con = duckdb.connect(':memory:')
    
    result = con.execute("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT transaction_id) as unique_transactions,
            SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) as negative_amounts,
            SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customers
        FROM read_csv_auto('s3://bucket/raw/transactions/date={{ ds }}/*.csv')
    """).fetchone()
    
    total_rows, unique_txns, neg_amounts, null_customers = result
    
    if total_rows < 100:
        raise ValueError(f"Too few transactions: {total_rows}")
    if neg_amounts > 0:
        raise ValueError(f"Found {neg_amounts} negative amounts")
    if null_customers > 0:
        raise ValueError(f"Found {null_customers} null customer IDs")
    
    logger.info(f"✓ Data quality passed: {total_rows} rows, {unique_txns} unique transactions")
    con.close()

with DAG(
    'retail_analytics_production',
    default_args={
        'owner': 'sho',
        'depends_on_past': False,
        'email': ['sho@company.com'],
        'email_on_failure': True,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='Production retail analytics pipeline',
    schedule_interval='0 3 * * *',  # 3 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['retail', 'transaction', 'dbt']
) as dag:
    

    start = EmptyOperator(task_id='start')
    
    validate_raw_data = PythonOperator(
        task_id='validate_raw_data',
        python_callable=validate_data_quality
    )
    

    dbt_staging_start = EmptyOperator(task_id='dbt_staging_start')
    
    dbt_run_stg_transactions = BashOperator(
        task_id='dbt_run_stg_transactions',
        bash_command='cd /opt/airflow/dbt && dbt run --select stg_transactions --profiles-dir .'
    )
    
    dbt_run_stg_customers = BashOperator(
        task_id='dbt_run_stg_customers',
        bash_command='cd /opt/airflow/dbt && dbt run --select stg_customers --profiles-dir .'
    )
    
    dbt_run_stg_products = BashOperator(
        task_id='dbt_run_stg_products',
        bash_command='cd /opt/airflow/dbt && dbt run --select stg_products --profiles-dir .'
    )
    
    dbt_test_staging = BashOperator(
        task_id='dbt_test_staging',
        bash_command='cd /opt/airflow/dbt && dbt test --select staging.* --profiles-dir .'
    )
    

    dbt_run_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command='cd /opt/airflow/dbt && dbt run --select intermediate.* --profiles-dir .'
    )
    

    dbt_marts_start = EmptyOperator(task_id='dbt_marts_start')
    
    dbt_run_dim_customers = BashOperator(
        task_id='dbt_run_dim_customers',
        bash_command='cd /opt/airflow/dbt && dbt run --select dim_customers --profiles-dir .'
    )
    
    dbt_run_dim_products = BashOperator(
        task_id='dbt_run_dim_products',
        bash_command='cd /opt/airflow/dbt && dbt run --select dim_products --profiles-dir .'
    )
    
    dbt_run_dim_stores = BashOperator(
        task_id='dbt_run_dim_stores',
        bash_command='cd /opt/airflow/dbt && dbt run --select dim_stores --profiles-dir .'
    )
    
    dbt_run_dim_time = BashOperator(
        task_id='dbt_run_dim_time',
        bash_command='cd /opt/airflow/dbt && dbt run --select dim_time --profiles-dir .'
    )
    

    dbt_facts_start = EmptyOperator(task_id='dbt_facts_start')
    
    dbt_run_fact_orders = BashOperator(
        task_id='dbt_run_fact_orders',
        bash_command='cd /opt/airflow/dbt && dbt run --select fact_orders --profiles-dir .'
    )
    
    dbt_run_fact_inventory = BashOperator(
        task_id='dbt_run_fact_inventory',
        bash_command='cd /opt/airflow/dbt && dbt run --select fact_inventory --profiles-dir .'
    )
    
    dbt_test_marts = BashOperator(
        task_id='dbt_test_marts',
        bash_command='cd /opt/airflow/dbt && dbt test --select marts.* --profiles-dir .'
    )
    

    dbt_analytics_start = EmptyOperator(task_id='dbt_analytics_start')
    
    dbt_run_customer_ltv = BashOperator(
        task_id='dbt_run_customer_ltv',
        bash_command='cd /opt/airflow/dbt && dbt run --select customer_lifetime_value --profiles-dir .'
    )
    
    dbt_run_product_performance = BashOperator(
        task_id='dbt_run_product_performance',
        bash_command='cd /opt/airflow/dbt && dbt run --select product_performance --profiles-dir .'
    )
    
    dbt_run_sales_metrics = BashOperator(
        task_id='dbt_run_sales_metrics',
        bash_command='cd /opt/airflow/dbt && dbt run --select daily_sales_metrics --profiles-dir .'
    )
    
    dbt_test_analytics = BashOperator(
        task_id='dbt_test_analytics',
        bash_command='cd /opt/airflow/dbt && dbt test --select analytics.* --profiles-dir .'
    )
    

    dbt_docs_generate = BashOperator(
        task_id='dbt_docs_generate',
        bash_command='cd /opt/airflow/dbt && dbt docs generate --profiles-dir .'
    )
    
    end = EmptyOperator(task_id='end')
    

    start >> validate_raw_data >> dbt_staging_start
    
    dbt_staging_start >> [dbt_run_stg_transactions, dbt_run_stg_customers, dbt_run_stg_products]
    [dbt_run_stg_transactions, dbt_run_stg_customers, dbt_run_stg_products] >> dbt_test_staging
    
    dbt_test_staging >> dbt_run_intermediate >> dbt_marts_start
    
    dbt_marts_start >> [dbt_run_dim_customers, dbt_run_dim_products, dbt_run_dim_stores, dbt_run_dim_time]
    [dbt_run_dim_customers, dbt_run_dim_products, dbt_run_dim_stores, dbt_run_dim_time] >> dbt_facts_start
    
    dbt_facts_start >> [dbt_run_fact_orders, dbt_run_fact_inventory]
    [dbt_run_fact_orders, dbt_run_fact_inventory] >> dbt_test_marts
    
    dbt_test_marts >> dbt_analytics_start
    dbt_analytics_start >> [dbt_run_customer_ltv, dbt_run_product_performance, dbt_run_sales_metrics]
    [dbt_run_customer_ltv, dbt_run_product_performance, dbt_run_sales_metrics] >> dbt_test_analytics
    
    dbt_test_analytics >> dbt_docs_generate >> end