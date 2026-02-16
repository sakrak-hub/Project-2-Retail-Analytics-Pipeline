from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging
import duckdb
import subprocess

logger = logging.getLogger(__name__)

def run_silver_with_schema_detection(**context):

    ti = context['ti']
    
    cmd = "cd /opt/airflow/dbt && dbt run --select staging --profiles-dir /opt/airflow/dbt"
    
    result = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        timeout=600
    )
    
    output = result.stdout + result.stderr

    is_schema_change = (
        'source and target schemas' in output and
        'out of sync' in output
    )

    if is_schema_change:
        logger.warning("🔄 SCHEMA CHANGE DETECTED - Branching to reconciliation")
        ti.xcom_push(key='schema_change_detected', value=True)
        ti.xcom_push(key='schema_change_time', value=str(context['logical_date']))
        return 'schema_reconcile_silver'
    
    if result.returncode == 0:
        logger.info("✅ Staging run successful")
        ti.xcom_push(key='schema_change_detected', value=False)
        return 'continue_pipeline'

    logger.error(f"Staging run failed: {output}")
    raise ValueError("Staging run failed")

def reconcile_silver_schema(**context):

    logger.info("🔧 Running schema reconciliation (full_refresh)")
    
    cmd = "cd /opt/airflow/dbt && dbt run --select staging --full-refresh --profiles-dir /opt/airflow/dbt"
    
    result = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        timeout=600
    )
    
    if result.returncode == 0:
        logger.info("✅ Schema reconciled successfully")
    else:
        raise ValueError("Schema reconciliation failed")

def log_schema_change(**context):

    import duckdb
    
    ti = context['ti']
    
    schema_change = ti.xcom_pull(
        task_ids='run_silver_with_detection',
        key='schema_change_detected'
    )
    
    if not schema_change:
        return 
    
    schema_time = ti.xcom_pull(
        task_ids='run_silver_with_detection',
        key='schema_change_time'
    )

    dag_run_id = context['dag_run'].run_id
    
    conn = duckdb.connect('/opt/airflow/dbt/warehouse.duckdb')

    try:

        conn.execute("""
            CREATE TABLE IF NOT EXISTS retail_transactions_data.schema_change_log (
                log_id INTEGER PRIMARY KEY,
                model_name VARCHAR NOT NULL,
                detected_at TIMESTAMP NOT NULL,
                reconciliation_method VARCHAR NOT NULL,
                reconciliation_status VARCHAR NOT NULL,
                dag_run_id VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        result = conn.execute("""
            SELECT COALESCE(MAX(log_id), 0) + 1 
            FROM retail_transactions_data.schema_change_log
        """).fetchone()
        
        next_id = result[0]

        conn.execute("""
            INSERT INTO retail_transactions_data.schema_change_log 
            (log_id, model_name, detected_at, reconciliation_method, reconciliation_status, dag_run_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [next_id, 'staging_transactions', schema_time, 'full_refresh', 'completed', dag_run_id])
        
        logger.info("✅ Schema change logged to database")
    
    except Exception as e:
        logger.error(f"Failed to log schema change: {e}")
        
    finally:
        conn.close()


def check_silver_quality_gate(**context):
    
    ti = context['ti']
    db_path = '/opt/airflow/dbt/warehouse.duckdb'
    conn = duckdb.connect(db_path, read_only=True)
    
    try:
        query = """
        SELECT 
            total_line_items,
            unique_txn_product_combinations,
            avg_line_items_per_transaction,
            line_item_quality_score,
            duplicate_line_item_pct,
            health_status,
            quality_tier
        FROM retail_transactions_data.staging_transactions_profile
        WHERE profile_date = CURRENT_DATE
        """
        
        result = conn.execute(query).fetchone()
        
        if not result:
            logger.error("❌ No profile data found for today")
            raise ValueError("No bronze profile data available")
        
        (total_lines, unique_combos, avg_items, score, 
         dup_pct, health, tier) = result
        
        logger.info("="*60)
        logger.info("BRONZE QUALITY GATE CHECK")
        logger.info("="*60)
        logger.info(f"Total Line Items:              {total_lines:,}")
        logger.info(f"Unique (txn, product) Combos:  {unique_combos:,}")
        logger.info(f"Avg Items per Transaction:     {avg_items}")
        logger.info(f"Quality Score:                 {score:.1f} / 100")
        logger.info(f"Duplicate Percentage:          {dup_pct}%")
        logger.info(f"Health Status:                 {health}")
        logger.info(f"Quality Tier:                  {tier}")
        logger.info("")
        
        BRONZE_MIN_QUALITY_SCORE = 70.0
        BRONZE_MAX_DUPLICATE_PCT = 30.0
        BRONZE_MIN_AVG_ITEMS = 1.0
        BRONZE_MAX_AVG_ITEMS = 5.0
        
        ti.xcom_push(key='bronze_quality_score', value=float(score))
        ti.xcom_push(key='bronze_duplicate_pct', value=float(dup_pct))
        ti.xcom_push(key='bronze_total_lines', value=int(total_lines))
        ti.xcom_push(key='bronze_health_status', value=health)
        ti.xcom_push(key='bronze_quality_tier', value=tier)

        passed = (
            score >= BRONZE_MIN_QUALITY_SCORE and
            dup_pct <= BRONZE_MAX_DUPLICATE_PCT and
            avg_items >= BRONZE_MIN_AVG_ITEMS and
            avg_items <= BRONZE_MAX_AVG_ITEMS
        )
        
        if passed:
            logger.info("✅ QUALITY GATE PASSED")
            logger.info(f"   Quality Score: {score:.1f} >= {BRONZE_MIN_QUALITY_SCORE}")
            logger.info(f"   Duplicates: {dup_pct}% <= {BRONZE_MAX_DUPLICATE_PCT}%")
            logger.info(f"   Avg Items/Txn: {avg_items} in [{BRONZE_MIN_AVG_ITEMS}, {BRONZE_MAX_AVG_ITEMS}]")
            logger.info("="*60)
            logger.info("🚀 Continuing to Silver layer")
            return 'trigger_data_staging_modelling'
        else:
            logger.error("❌ QUALITY GATE FAILED")
            if score < BRONZE_MIN_QUALITY_SCORE:
                logger.error(f"   Quality too low: {score:.1f} < {BRONZE_MIN_QUALITY_SCORE}")
            if dup_pct > BRONZE_MAX_DUPLICATE_PCT:
                logger.error(f"   Too many duplicates: {dup_pct}% > {BRONZE_MAX_DUPLICATE_PCT}%")
            if avg_items < BRONZE_MIN_AVG_ITEMS or avg_items > BRONZE_MAX_AVG_ITEMS:
                logger.error(f"   Unusual grain: {avg_items} items/txn")
            logger.info("="*60)
            
            return 'skip_staging'
    
    finally:
        conn.close()

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
    description='Processing raw data for data warehouse and presentation',
    schedule=None, 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['retail', 'dbt', 'quality-gates', 'incremental', 'silver', 'gold']
) as dag:

    silver_layer_start = EmptyOperator(task_id='silver_layer_start')

    dbt_run_silver_with_detection = BranchPythonOperator(
        task_id='run_silver_with_detection',
        python_callable=run_silver_with_schema_detection
    )

    continue_pipeline = EmptyOperator(
        task_id='continue_pipeline'
    )

    schema_reconcile_silver = PythonOperator(
        task_id='schema_reconcile_silver',
        python_callable=reconcile_silver_schema
    )

    log_schema_change_task = PythonOperator(
        task_id='log_schema_change',
        python_callable=log_schema_change
    )

    dbt_test_silver = BashOperator( 
        task_id='dbt_test_silver',
        bash_command='cd /opt/airflow/dbt && dbt test --select staging --profiles-dir /opt/airflow/dbt',
        trigger_rule='none_failed_min_one_success'
    )

    dbt_create_silver_profile = BashOperator(
        task_id='dbt_create_silver_profile',
        bash_command='cd /opt/airflow/dbt && dbt run --select staging_transactions_profile --profiles-dir /opt/airflow/dbt'
    )

    silver_quality_gate = BranchPythonOperator(
        task_id='silver_quality_gate',
        python_callable=check_silver_quality_gate,
    )

    silver_layer_start >> dbt_run_silver_with_detection

    dbt_run_silver_with_detection >> [continue_pipeline, schema_reconcile_silver]

    schema_reconcile_silver >> log_schema_change_task >> dbt_test_silver
    
    continue_pipeline >> dbt_test_silver >> dbt_create_silver_profile 
    # >> silver_quality_gate