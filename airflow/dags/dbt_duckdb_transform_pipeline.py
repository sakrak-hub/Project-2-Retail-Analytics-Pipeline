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

def check_bronze_quality_gate(**context):
    
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
        FROM retail_transactions_data.bronze_transactions_profile
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


def run_bronze_with_schema_detection(**context):

    ti = context['ti']
    
    cmd = "cd /opt/airflow/dbt && dbt run --select bronze --profiles-dir /opt/airflow/dbt"
    
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
        return 'schema_reconcile_bronze'
    
    if result.returncode == 0:
        logger.info("✅ Bronze run successful")
        ti.xcom_push(key='schema_change_detected', value=False)
        return 'continue_pipeline'

    logger.error(f"Bronze run failed: {output}")
    raise ValueError("Bronze run failed")

def reconcile_bronze_schema(**context):

    logger.info("🔧 Running schema reconciliation (full_refresh)")
    
    cmd = "cd /opt/airflow/dbt && dbt run --select bronze --full-refresh --profiles-dir /opt/airflow/dbt"
    
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
        task_ids='run_bronze_with_detection',
        key='schema_change_detected'
    )
    
    if not schema_change:
        return 
    
    schema_time = ti.xcom_pull(
        task_ids='run_bronze_with_detection',
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
        """, [next_id, 'bronze_transactions', schema_time, 'full_refresh', 'completed', dag_run_id])
        
        logger.info("✅ Schema change logged to database")
    
    except Exception as e:
        logger.error(f"Failed to log schema change: {e}")
        
    finally:
        conn.close()

def rename_column(**context):

    conn = duckdb.connect('/opt/airflow/dbt/warehouse.duckdb')

    query = """
    CREATE OR REPLACE TABLE retail_transactions_data.us_zip_fips_county AS
        SELECT * RENAME (
            "Zip Code" AS zip_code,
            "State Name" AS state_name,
            "State Abrv" AS state_abrv,
            "State Code" AS state_code,
            "County Name" AS county_name,
            "County Code" AS count_code,
            "FIPS Code" AS fips_code,
            "ANSI Code" AS ansi_code,
            "Centroid Lat" AS cent_lat,
            "Centroid Long" AS cent_long
        )
        FROM retail_transactions_data.us_zip_fips_county
    """

    try:
        conn.execute(query)
    except Exception as e:
        logger.error(f"Failed to change column names: {e}")
        
    finally:
        conn.close()

with DAG(
    'retail_analytics_dbt_duckdb_pipeline',
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

    start = EmptyOperator(task_id='pipeline_start')

    raw_layer_start = EmptyOperator(task_id='raw_layer_start')

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='cd /opt/airflow/dbt && dbt seed --profiles-dir /opt/airflow/dbt'
    )

    dbt_seed_column_rename = PythonOperator(
        task_id='dbt_seed_column_rename',
        python_callable = rename_column
    )

    dbt_run_raw = BashOperator(
        task_id='dbt_run_raw',
        bash_command='cd /opt/airflow/dbt && dbt run --select raw --profiles-dir /opt/airflow/dbt'
    )

    dbt_test_raw_sources = BashOperator(
        task_id='dbt_test_raw_sources',
        bash_command='cd /opt/airflow/dbt && dbt test --select source:* --profiles-dir /opt/airflow/dbt'
    )

    bronze_layer_start = EmptyOperator(task_id='bronze_layer_start')

    dbt_run_bronze_with_detection = BranchPythonOperator(
        task_id='run_bronze_with_detection',
        python_callable=run_bronze_with_schema_detection
    )

    continue_pipeline = EmptyOperator(
        task_id='continue_pipeline'
    )

    schema_reconcile_bronze = PythonOperator(
        task_id='schema_reconcile_bronze',
        python_callable=reconcile_bronze_schema
    )

    log_schema_change_task = PythonOperator(
        task_id='log_schema_change',
        python_callable=log_schema_change
    )

    dbt_test_bronze = BashOperator(
        task_id='dbt_test_bronze',
        bash_command='cd /opt/airflow/dbt && dbt test --select bronze --profiles-dir /opt/airflow/dbt',
        trigger_rule='none_failed_min_one_success'
    )

    dbt_create_bronze_profile = BashOperator(
        task_id='dbt_create_bronze_profile',
        bash_command='cd /opt/airflow/dbt && dbt run --select bronze_transactions_profile --profiles-dir /opt/airflow/dbt'
    )

    bronze_quality_gate = BranchPythonOperator(
        task_id='bronze_quality_gate',
        python_callable=check_bronze_quality_gate,
    )

    trigger_staging = TriggerDagRunOperator(
        task_id='trigger_data_staging_modelling',
        trigger_dag_id='retail_analytics_dbt_duckdb_staging_modelling',
        wait_for_completion=True,
        poke_interval=60,
        conf={
            'triggered_by': 'bronze_quality_gate',
            'execution_date': '{{ logical_date }}',
            'bronze_run_id': '{{ run_id }}',
            'bronze_quality_score': '{{ ti.xcom_pull(task_ids="bronze_quality_gate", key="bronze_quality_score") }}',
            'bronze_duplicate_pct': '{{ ti.xcom_pull(task_ids="bronze_quality_gate", key="bronze_duplicate_pct") }}',
            'bronze_total_lines': '{{ ti.xcom_pull(task_ids="bronze_quality_gate", key="bronze_total_lines") }}',
            'bronze_health_status': '{{ ti.xcom_pull(task_ids="bronze_quality_gate", key="bronze_health_status") }}',
            'bronze_quality_tier': '{{ ti.xcom_pull(task_ids="bronze_quality_gate", key="bronze_quality_tier") }}'
        },

        allowed_states=['success'],
        failed_states=['failed']
    )

    skip_staging = EmptyOperator(
        task_id='skip_staging'
    )

    pipeline_complete = EmptyOperator(
        task_id='pipeline_complete',
        trigger_rule='none_failed_min_one_success'
    )

    start >> raw_layer_start

    raw_layer_start >> dbt_seed >> dbt_seed_column_rename >> dbt_run_raw >> dbt_test_raw_sources

    dbt_test_raw_sources >> bronze_layer_start

    bronze_layer_start >> dbt_run_bronze_with_detection 

    dbt_run_bronze_with_detection >> [continue_pipeline, schema_reconcile_bronze]

    schema_reconcile_bronze >> log_schema_change_task >> dbt_test_bronze
    
    continue_pipeline >> dbt_test_bronze >> dbt_create_bronze_profile >> bronze_quality_gate 

    bronze_quality_gate >> [trigger_staging, skip_staging]

    trigger_staging >> pipeline_complete

    skip_staging >> pipeline_complete

    dag.doc_md = """
# Retail Analytics Pipeline with Quality Gates

## Flow

1. **Raw Layer**
   - Create raw views/tables
   - Test source data (validates incoming data)

2. **Bronze Layer**
   - Create bronze tables (with quality flags)
   - Test bronze data (validates transformations)
   - Create quality profile (aggregate metrics)
   - Check quality gate (≥70% score, ≤5% duplicates)

3. **Silver Layer** (only if quality gate passes)
   - Create staging tables (filtered & enriched)
   - (To be implemented)

## Quality Gates

- **Bronze:** Score ≥ 70, Duplicates ≤ 5%, Avg items/txn in [1, 5]
- **Silver:** (To be implemented)
- **Gold:** (To be implemented)

## Monitoring

- Check Airflow logs for quality metrics
- Query `bronze_transactions_profile` for trending
- Set up alerts on `quality_alert_flag`
"""