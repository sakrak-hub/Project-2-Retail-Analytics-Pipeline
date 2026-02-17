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


def check_silver_quality_gate(ds,logical_date,**context):

    logger.info(f"{ds}")
    logger.info(f"{logical_date}")

    ti = context['ti']
    
    db_path = '/opt/airflow/dbt/warehouse.duckdb'
    conn = duckdb.connect(db_path, read_only=True)
    
    try:
        query = f"""
        SELECT 
            total_line_items,
            unique_line_items,
            unique_transactions,
            unique_customers,
            avg_items_per_transaction,
            clean_records,
            clean_pct,
            corrected_pct,
            transaction_staging_quality_score,
            deduplication_status,
            deduplication_success_pct,
            health_status,
            quality_tier,
            refund_pct,
            total_line_item_revenue,
            quality_alert_flag
        FROM retail_transactions_data.staging_transactions_profile
        WHERE profile_date::DATE = CURRENT_DATE
        """
        
        result = conn.execute(query).fetchone()
        
        if not result:
            logger.error("❌ No staging profile data found for today")
            raise ValueError("No staging profile data available")
        
        (total_lines, unique_lines, unique_txns, unique_custs, 
         avg_items, clean_recs, clean_pct, corrected_pct, quality_score,
         dedup_status, dedup_pct, health, tier, refund_pct, 
         revenue, alert_flag) = result
        
        logger.info("="*60)
        logger.info("SILVER/STAGING QUALITY GATE CHECK")
        logger.info("="*60)
        logger.info(f"Total Line Items:              {total_lines:,}")
        logger.info(f"Unique Line Items:             {unique_lines:,}")
        logger.info(f"Unique Transactions:           {unique_txns:,}")
        logger.info(f"Unique Customers:              {unique_custs:,}")
        logger.info(f"Avg Items per Transaction:     {avg_items:.2f}")
        logger.info("")
        logger.info(f"Clean Records:                 {clean_recs:,} ({clean_pct:.1f}%)")
        logger.info(f"Corrected Records:             {corrected_pct:.1f}%")
        logger.info(f"Quality Score:                 {quality_score:.1f} / 100")
        logger.info("")
        logger.info(f"Deduplication Status:          {dedup_status}")
        logger.info(f"Deduplication Success:         {dedup_pct:.1f}%")
        logger.info("")
        logger.info(f"Health Status:                 {health}")
        logger.info(f"Quality Tier:                  {tier}")
        logger.info(f"Refund Percentage:             {refund_pct:.1f}%")
        logger.info(f"Total Revenue:                 ${revenue:,.2f}")
        logger.info(f"Quality Alert:                 {'⚠️ YES' if alert_flag else '✅ NO'}")
        logger.info("")
        
        STAGING_REQUIRED_DEDUP_PCT = 100.0
        
        STAGING_MIN_QUALITY_SCORE = 90.0
        
        STAGING_MIN_CLEAN_PCT = 90.0
        
        STAGING_MIN_VOLUME = 45000
        STAGING_MAX_VOLUME = 50500
        
        STAGING_MAX_REFUND_PCT = 10.0
        STAGING_MIN_AVG_ITEMS = 1.5
        STAGING_MAX_AVG_ITEMS = 3.0
        
        ti.xcom_push(key='staging_quality_score', value=float(quality_score))
        ti.xcom_push(key='staging_clean_pct', value=float(clean_pct))
        ti.xcom_push(key='staging_dedup_pct', value=float(dedup_pct))
        ti.xcom_push(key='staging_total_lines', value=int(total_lines))
        ti.xcom_push(key='staging_unique_txns', value=int(unique_txns))
        ti.xcom_push(key='staging_health_status', value=health)
        ti.xcom_push(key='staging_quality_tier', value=tier)
        ti.xcom_push(key='staging_refund_pct', value=float(refund_pct))
        ti.xcom_push(key='staging_revenue', value=float(revenue))
        
        dedup_perfect = (dedup_pct == STAGING_REQUIRED_DEDUP_PCT)
        
        volume_ok = (STAGING_MIN_VOLUME <= total_lines <= STAGING_MAX_VOLUME)
        
        quality_ok = (quality_score >= STAGING_MIN_QUALITY_SCORE)
        
        clean_ok = (clean_pct >= STAGING_MIN_CLEAN_PCT)
        
        refund_ok = (refund_pct <= STAGING_MAX_REFUND_PCT)
        
        avg_items_ok = (STAGING_MIN_AVG_ITEMS <= avg_items <= STAGING_MAX_AVG_ITEMS)
        
        unique_ok = (total_lines == unique_lines)

        passed = (
            dedup_perfect and
            volume_ok and
            quality_ok and
            clean_ok and
            refund_ok and
            avg_items_ok and
            unique_ok
        )
        
        if passed:
            logger.info("✅ STAGING QUALITY GATE PASSED")
            logger.info("="*60)
            logger.info("ALL CHECKS PASSED:")
            logger.info(f"   ✅ Deduplication:       {dedup_pct:.1f}% = {STAGING_REQUIRED_DEDUP_PCT}%")
            logger.info(f"   ✅ Volume:              {total_lines:,} in [{STAGING_MIN_VOLUME:,}, {STAGING_MAX_VOLUME:,}]")
            logger.info(f"   ✅ Quality Score:       {quality_score:.1f} >= {STAGING_MIN_QUALITY_SCORE}")
            logger.info(f"   ✅ Clean Data:          {clean_pct:.1f}% >= {STAGING_MIN_CLEAN_PCT}%")
            logger.info(f"   ✅ Refunds:             {refund_pct:.1f}% <= {STAGING_MAX_REFUND_PCT}%")
            logger.info(f"   ✅ Avg Items/Txn:       {avg_items:.2f} in [{STAGING_MIN_AVG_ITEMS}, {STAGING_MAX_AVG_ITEMS}]")
            logger.info(f"   ✅ Unique = Total:      {unique_lines:,} = {total_lines:,}")
            logger.info("="*60)
            logger.info("🚀 Proceeding to Gold Layer")
            
            return 'trigger_gold_modelling'
        
        else:
            logger.warning("⚠️ STAGING QUALITY GATE NOT MET - SKIPPING GOLD LAYER")
            logger.warning("="*60)
            logger.warning("FAILED CHECKS:")
            
            if not dedup_perfect:
                logger.error(f"   ❌ CRITICAL: Deduplication not perfect: {dedup_pct:.1f}% != {STAGING_REQUIRED_DEDUP_PCT}%")
                logger.error(f"      Status: {dedup_status}")
            
            if not unique_ok:
                logger.error(f"   ❌ CRITICAL: Duplicates exist: {total_lines:,} total != {unique_lines:,} unique")
                logger.error(f"      Duplicates: {total_lines - unique_lines:,}")
            
            if not volume_ok:
                logger.warning(f"   ⚠️  Volume out of range: {total_lines:,} not in [{STAGING_MIN_VOLUME:,}, {STAGING_MAX_VOLUME:,}]")
            
            if not quality_ok:
                logger.warning(f"   ⚠️  Quality too low: {quality_score:.1f} < {STAGING_MIN_QUALITY_SCORE}")
            
            if not clean_ok:
                logger.warning(f"   ⚠️  Clean % too low: {clean_pct:.1f}% < {STAGING_MIN_CLEAN_PCT}%")
            
            if not refund_ok:
                logger.warning(f"   ⚠️  Refunds too high: {refund_pct:.1f}% > {STAGING_MAX_REFUND_PCT}%")
            
            if not avg_items_ok:
                logger.warning(f"   ⚠️  Unusual grain: {avg_items:.2f} items/txn not in [{STAGING_MIN_AVG_ITEMS}, {STAGING_MAX_AVG_ITEMS}]")
            
            logger.warning("="*60)
            logger.warning("⏭️  Skipping Gold layer - Staging quality below threshold")
            logger.warning("   Staging data is available but won't proceed to dimensional modelling")
            
            return 'skip_gold'
    
    finally:
        conn.close()

with DAG(
    'retail_analytics_dbt_duckdb_staging',
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
    catchup=True,
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

    trigger_staging = TriggerDagRunOperator(
        task_id='trigger_data_modelling',
        trigger_dag_id='retail_analytics_dbt_duckdb_modelling',
        wait_for_completion=True,
        poke_interval=60,
        conf={
            'triggered_by': 'silver_quality_gate',
            'execution_date': '{{ logical_date }}',
            'silver_run_id': '{{ run_id }}',
            'staging_quality_score': '{{ ti.xcom_pull(task_ids="silver_quality_gate", key="staging_quality_score") }}',
            'staging_clean_pct': '{{ ti.xcom_pull(task_ids="silver_quality_gate", key="staging_clean_pct") }}',
            'staging_dedup_pct': '{{ ti.xcom_pull(task_ids="silver_quality_gate", key="staging_dedup_pct") }}',
            'staging_total_lines': '{{ ti.xcom_pull(task_ids="silver_quality_gate", key="staging_total_lines") }}',
            'staging_unique_txns': '{{ ti.xcom_pull(task_ids="silver_quality_gate", key="staging_unique_txns") }}',
            'staging_health_status': '{{ ti.xcom_pull(task_ids="silver_quality_gate", key="staging_health_status") }}',
            'staging_quality_tier': '{{ ti.xcom_pull(task_ids="silver_quality_gate", key="staging_quality_tier") }}',
            'staging_refund_pct': '{{ ti.xcom_pull(task_ids="silver_quality_gate", key="bronze_quality_tier") }}',
            'staging_revenue': '{{ ti.xcom_pull(task_ids="silver_quality_gate", key="bronze_quality_tier") }}'
        },


        allowed_states=['success'],
        failed_states=['failed']
    )

    skip_mart = EmptyOperator(
        task_id='skip_mart'
    )

    staging_pipeline_complete = EmptyOperator(
        task_id='staging_pipeline_complete',
        trigger_rule='none_failed_min_one_success'
    )


    silver_layer_start >> dbt_run_silver_with_detection

    dbt_run_silver_with_detection >> [continue_pipeline, schema_reconcile_silver]

    schema_reconcile_silver >> log_schema_change_task >> dbt_test_silver
    
    continue_pipeline >> dbt_test_silver >> dbt_create_silver_profile >> silver_quality_gate

    silver_quality_gate >> [trigger_staging, skip_mart]

    trigger_staging >> staging_pipeline_complete

    skip_mart >> staging_pipeline_complete