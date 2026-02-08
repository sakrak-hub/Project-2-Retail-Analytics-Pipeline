from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from loguru import logger
import duckdb


# ========================================
# QUALITY GATE FUNCTIONS
# ========================================

def check_bronze_quality(**context):
    """
    Quality Gate #1: Check if bronze data quality is acceptable
    Fails if:
    - Quality score < 70%
    - Total rows < 100
    - Quality tier = 'POOR'
    """
    import duckdb
    
    db_path = '/opt/airflow/dbt/datawarehouse.duckdb'
    con = duckdb.connect(db_path, read_only=True)
    
    try:
        result = con.execute("""
            SELECT 
                overall_quality_score,
                total_rows,
                quality_tier
            FROM bronze_transactions_profile
            ORDER BY profiled_at DESC
            LIMIT 1
        """).fetchone()
        
        if not result:
            logger.error("❌ No bronze quality profile found")
            raise ValueError("Bronze quality profile not found - run bronze_transactions_profile first")
        
        quality_score, total_rows, quality_tier = result
        
        logger.info(f"📊 Bronze Quality Metrics:")
        logger.info(f"   - Quality Score: {quality_score}%")
        logger.info(f"   - Total Rows: {total_rows}")
        logger.info(f"   - Quality Tier: {quality_tier}")
        
        # Check quality thresholds
        if quality_score < 70:
            logger.error(f"❌ STOP: Quality score too poor ({quality_score}% < 70%)")
            raise ValueError(f"Bronze quality score {quality_score}% is below threshold of 70%")
        
        if total_rows < 100:
            logger.error(f"❌ STOP: Insufficient data ({total_rows} < 100 rows)")
            raise ValueError(f"Insufficient data: only {total_rows} rows")
        
        if quality_tier == 'POOR':
            logger.error(f"❌ STOP: Quality tier is POOR")
            raise ValueError("Bronze data quality tier is POOR - manual review required")
        
        logger.info(f"✅ Bronze quality gate PASSED")
        logger.info(f"   Quality acceptable - proceeding to staging")
        
    finally:
        con.close()


def check_staging_quality(**context):
    """
    Quality Gate #2: Check if staging data quality is acceptable
    Fails if:
    - Clean percentage < 90%
    """
    import duckdb
    
    db_path = '/opt/airflow/dbt/datawarehouse.duckdb'
    con = duckdb.connect(db_path, read_only=True)
    
    try:
        result = con.execute("""
            SELECT 
                clean_percentage,
                total_records,
                clean_records,
                acceptable_records,
                poor_records
            FROM staging_quality_summary
            ORDER BY quality_check_timestamp DESC
            LIMIT 1
        """).fetchone()
        
        if not result:
            logger.error("❌ No staging quality summary found")
            raise ValueError("Staging quality summary not found")
        
        clean_pct, total_records, clean_records, acceptable_records, poor_records = result
        
        logger.info(f"📊 Staging Quality Metrics:")
        logger.info(f"   - Total Records: {total_records}")
        logger.info(f"   - Clean: {clean_records} ({clean_pct}%)")
        logger.info(f"   - Acceptable: {acceptable_records}")
        logger.info(f"   - Poor: {poor_records}")
        
        if clean_pct < 90:
            logger.error(f"❌ STOP: Clean percentage too low ({clean_pct}% < 90%)")
            raise ValueError(f"Staging clean percentage {clean_pct}% is below threshold of 90%")
        
        logger.info(f"✅ Staging quality gate PASSED")
        logger.info(f"   {clean_pct}% clean records - proceeding to dimensions")
        
    finally:
        con.close()


def check_reconciliation(**context):
    """
    Quality Gate #3: Check end-to-end reconciliation
    Warns if data loss > 5% but doesn't fail
    """
    import duckdb
    
    db_path = '/opt/airflow/dbt/datawarehouse.duckdb'
    con = duckdb.connect(db_path, read_only=True)
    
    try:
        result = con.execute("""
            WITH latest_batch AS (
                SELECT 
                    b.record_count as bronze_records,
                    s.record_count as staging_records,
                    f.record_count as fact_records,
                    b.total_amount as bronze_amount,
                    s.total_amount as staging_amount,
                    f.total_amount as fact_amount
                FROM end_to_end_reconciliation b
                JOIN end_to_end_reconciliation s ON b.batch_id = s.batch_id AND s.layer = 'SILVER_STAGING'
                JOIN end_to_end_reconciliation f ON b.batch_id = f.batch_id AND f.layer = 'GOLD_FACTS'
                WHERE b.layer = 'BRONZE_SOURCE'
                ORDER BY b.reconciled_at DESC
                LIMIT 1
            )
            SELECT 
                bronze_records,
                staging_records,
                fact_records,
                ROUND((bronze_records - fact_records) * 100.0 / bronze_records, 2) as data_loss_pct,
                ABS(bronze_amount - fact_amount) as amount_difference
            FROM latest_batch
        """).fetchone()
        
        if result:
            bronze_records, staging_records, fact_records, data_loss_pct, amount_diff = result
            
            logger.info(f"📊 Reconciliation Metrics:")
            logger.info(f"   - Bronze Records: {bronze_records}")
            logger.info(f"   - Staging Records: {staging_records}")
            logger.info(f"   - Fact Records: {fact_records}")
            logger.info(f"   - Data Loss: {data_loss_pct}%")
            logger.info(f"   - Amount Difference: ${amount_diff:.2f}")
            
            if data_loss_pct > 5.0:
                logger.warning(f"⚠️  WARNING: Data loss {data_loss_pct}% exceeds 5% threshold")
            else:
                logger.info(f"✅ Reconciliation PASSED - data loss within acceptable range")
        else:
            logger.warning("⚠️  No reconciliation data found")
            
    finally:
        con.close()


# ========================================
# DAG DEFINITION
# ========================================

with DAG(
    'retail_analytics_quality_pipeline',
    default_args={
        'owner': 'sho',
        'depends_on_past': False,
        'email': ['sakra_k@outlook.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='Production retail analytics pipeline with quality gates',
    schedule='0 0 * * *', 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['retail', 'dbt', 'quality-gates', 'incremental']
) as dag:
    
    start = EmptyOperator(task_id='pipeline_start')
    
    # ========================================
    # PHASE 1: BRONZE LAYER (Log, Don't Block)
    # ========================================
    
    bronze_layer_start = EmptyOperator(task_id='bronze_layer_start')
    
    dbt_run_bronze = BashOperator(
        task_id='dbt_run_bronze',
        bash_command='cd /opt/airflow/dbt && dbt run --models bronze --profiles-dir .'
    )
    
    # Test bronze with warn-only mode (only fail on severity=error)
    dbt_test_bronze_sources = BashOperator(
        task_id='dbt_test_bronze_sources',
        bash_command='cd /opt/airflow/dbt && dbt test --select source:* --warn-error-options \'{"include": "error"}\' --profiles-dir .'
    )
    
    # Profile bronze data quality
    dbt_run_bronze_profile = BashOperator(
        task_id='dbt_run_bronze_profile',
        bash_command='cd /opt/airflow/dbt && dbt run --models bronze_transactions_profile --profiles-dir .'
    )
    
    # ========================================
    # QUALITY GATE #1: Bronze Quality Check
    # ========================================
    
    bronze_quality_gate = PythonOperator(
        task_id='bronze_quality_gate',
        python_callable=check_bronze_quality,
    )
    
    # ========================================
    # PHASE 2: STAGING LAYER (Clean & Validate)
    # ========================================
    
    staging_layer_start = EmptyOperator(task_id='staging_layer_start')
    
    # Run staging models in parallel
    dbt_run_stg_transactions = BashOperator(
        task_id='dbt_run_stg_transactions',
        bash_command='cd /opt/airflow/dbt && dbt run --models stg_retail_transactions --profiles-dir .'
    )
    
    dbt_run_stg_customers = BashOperator(
        task_id='dbt_run_stg_customers',
        bash_command='cd /opt/airflow/dbt && dbt run --models stg_customers --profiles-dir .'
    )
    
    dbt_run_stg_products = BashOperator(
        task_id='dbt_run_stg_products',
        bash_command='cd /opt/airflow/dbt && dbt run --models stg_products --profiles-dir .'
    )
    
    dbt_run_stg_stores = BashOperator(
        task_id='dbt_run_stg_stores',
        bash_command='cd /opt/airflow/dbt && dbt run --models stg_stores --profiles-dir .'
    )
    
    staging_complete = EmptyOperator(task_id='staging_complete')
    
    # Generate staging quality summary
    dbt_run_staging_quality_summary = BashOperator(
        task_id='dbt_run_staging_quality_summary',
        bash_command='cd /opt/airflow/dbt && dbt run --models staging_quality_summary --profiles-dir .'
    )
    
    # Test staging with strict error mode
    dbt_test_staging = BashOperator(
        task_id='dbt_test_staging',
        bash_command='cd /opt/airflow/dbt && dbt test --select staging --warn-error --profiles-dir .'
    )
    
    # ========================================
    # QUALITY GATE #2: Staging Quality Check
    # ========================================
    
    staging_quality_gate = PythonOperator(
        task_id='staging_quality_gate',
        python_callable=check_staging_quality,
    )
    
    # ========================================
    # PHASE 3: PRE-DIMENSIONAL VALIDATION
    # ========================================
    
    validation_layer_start = EmptyOperator(task_id='validation_layer_start')
    
    dbt_run_control_totals = BashOperator(
        task_id='dbt_run_control_totals',
        bash_command='cd /opt/airflow/dbt && dbt run --models pre_dimensional_control_totals --profiles-dir .'
    )
    
    dbt_run_dimension_reference_check = BashOperator(
        task_id='dbt_run_dimension_reference_check',
        bash_command='cd /opt/airflow/dbt && dbt run --models dimension_reference_check --profiles-dir .'
    )
    
    validation_complete = EmptyOperator(task_id='validation_complete')
    
    # ========================================
    # PHASE 4: DIMENSIONS (SCD Type 2)
    # ========================================
    
    dimensions_layer_start = EmptyOperator(task_id='dimensions_layer_start')
    
    dbt_run_dim_customers = BashOperator(
        task_id='dbt_run_dim_customers',
        bash_command='cd /opt/airflow/dbt && dbt run --models dim_customer --profiles-dir .'
    )
    
    dbt_run_dim_products = BashOperator(
        task_id='dbt_run_dim_products',
        bash_command='cd /opt/airflow/dbt && dbt run --models dim_product --profiles-dir .'
    )
    
    dbt_run_dim_stores = BashOperator(
        task_id='dbt_run_dim_stores',
        bash_command='cd /opt/airflow/dbt && dbt run --models dim_store --profiles-dir .'
    )
    
    dbt_run_dim_date = BashOperator(
        task_id='dbt_run_dim_date',
        bash_command='cd /opt/airflow/dbt && dbt run --models dim_date --profiles-dir .'
    )
    
    dimensions_complete = EmptyOperator(task_id='dimensions_complete')
    
    # Test dimensions with strict error mode
    dbt_test_dimensions = BashOperator(
        task_id='dbt_test_dimensions',
        bash_command='cd /opt/airflow/dbt && dbt test --select dimensions --warn-error --profiles-dir .'
    )
    
    # ========================================
    # PHASE 5: FACTS (Incremental Append)
    # ========================================
    
    facts_layer_start = EmptyOperator(task_id='facts_layer_start')
    
    dbt_run_fact_sales = BashOperator(
        task_id='dbt_run_fact_sales',
        bash_command='cd /opt/airflow/dbt && dbt run --models fact_sales --profiles-dir .'
    )
    
    dbt_run_fact_orders = BashOperator(
        task_id='dbt_run_fact_orders',
        bash_command='cd /opt/airflow/dbt && dbt run --models fact_orders --profiles-dir .'
    )
    
    dbt_run_fact_inventory = BashOperator(
        task_id='dbt_run_fact_inventory',
        bash_command='cd /opt/airflow/dbt && dbt run --models fact_inventory --profiles-dir .'
    )
    
    facts_complete = EmptyOperator(task_id='facts_complete')
    
    # Test facts with strict error mode
    dbt_test_facts = BashOperator(
        task_id='dbt_test_facts',
        bash_command='cd /opt/airflow/dbt && dbt test --select facts --warn-error --profiles-dir .'
    )
    
    # ========================================
    # PHASE 6: RECONCILIATION
    # ========================================
    
    reconciliation_layer_start = EmptyOperator(task_id='reconciliation_layer_start')
    
    dbt_run_reconciliation = BashOperator(
        task_id='dbt_run_reconciliation',
        bash_command='cd /opt/airflow/dbt && dbt run --models end_to_end_reconciliation --profiles-dir .'
    )
    
    # Test for data loss
    dbt_test_data_loss = BashOperator(
        task_id='dbt_test_data_loss',
        bash_command='cd /opt/airflow/dbt && dbt test --select assert_no_incremental_data_loss --warn-error --profiles-dir .'
    )
    
    # Quality Gate #3: Check reconciliation
    reconciliation_check = PythonOperator(
        task_id='reconciliation_check',
        python_callable=check_reconciliation,
    )
    
    # ========================================
    # PHASE 7: ANALYTICS MODELS
    # ========================================
    
    analytics_layer_start = EmptyOperator(task_id='analytics_layer_start')
    
    dbt_run_customer_ltv = BashOperator(
        task_id='dbt_run_customer_ltv',
        bash_command='cd /opt/airflow/dbt && dbt run --models customer_lifetime_value --profiles-dir .'
    )
    
    dbt_run_product_performance = BashOperator(
        task_id='dbt_run_product_performance',
        bash_command='cd /opt/airflow/dbt && dbt run --models product_performance --profiles-dir .'
    )
    
    dbt_run_sales_metrics = BashOperator(
        task_id='dbt_run_sales_metrics',
        bash_command='cd /opt/airflow/dbt && dbt run --models daily_sales_metrics --profiles-dir .'
    )
    
    analytics_complete = EmptyOperator(task_id='analytics_complete')
    
    # Test analytics
    dbt_test_analytics = BashOperator(
        task_id='dbt_test_analytics',
        bash_command='cd /opt/airflow/dbt && dbt test --select analytics --profiles-dir .'
    )
    
    # ========================================
    # PHASE 8: DOCUMENTATION & MONITORING
    # ========================================
    
    monitoring_layer_start = EmptyOperator(task_id='monitoring_layer_start')
    
    # Generate data quality dashboard
    dbt_run_quality_dashboard = BashOperator(
        task_id='dbt_run_quality_dashboard',
        bash_command='cd /opt/airflow/dbt && dbt run --models data_quality_dashboard --profiles-dir .'
    )
    
    # Generate quality trend analysis
    dbt_run_quality_trends = BashOperator(
        task_id='dbt_run_quality_trends',
        bash_command='cd /opt/airflow/dbt && dbt run --models quality_trend_analysis --profiles-dir .'
    )
    
    monitoring_complete = EmptyOperator(task_id='monitoring_complete')
    
    # Generate dbt documentation
    dbt_docs_generate = BashOperator(
        task_id='dbt_docs_generate',
        bash_command='cd /opt/airflow/dbt && dbt docs generate --profiles-dir .'
    )
    
    pipeline_end = EmptyOperator(task_id='pipeline_end')
    
    # ========================================
    # TASK DEPENDENCIES
    # ========================================
    
    # Phase 1: Bronze Layer
    start >> bronze_layer_start
    bronze_layer_start >> dbt_run_bronze
    dbt_run_bronze >> dbt_test_bronze_sources
    dbt_test_bronze_sources >> dbt_run_bronze_profile
    dbt_run_bronze_profile >> bronze_quality_gate
    
    # Phase 2: Staging Layer
    bronze_quality_gate >> staging_layer_start
    staging_layer_start >> [dbt_run_stg_transactions, dbt_run_stg_customers, dbt_run_stg_products, dbt_run_stg_stores]
    [dbt_run_stg_transactions, dbt_run_stg_customers, dbt_run_stg_products, dbt_run_stg_stores] >> staging_complete
    staging_complete >> dbt_run_staging_quality_summary
    dbt_run_staging_quality_summary >> dbt_test_staging
    dbt_test_staging >> staging_quality_gate
    
    # Phase 3: Pre-Dimensional Validation
    staging_quality_gate >> validation_layer_start
    validation_layer_start >> [dbt_run_control_totals, dbt_run_dimension_reference_check]
    [dbt_run_control_totals, dbt_run_dimension_reference_check] >> validation_complete
    
    # Phase 4: Dimensions
    validation_complete >> dimensions_layer_start
    dimensions_layer_start >> [dbt_run_dim_customers, dbt_run_dim_products, dbt_run_dim_stores, dbt_run_dim_date]
    [dbt_run_dim_customers, dbt_run_dim_products, dbt_run_dim_stores, dbt_run_dim_date] >> dimensions_complete
    dimensions_complete >> dbt_test_dimensions
    
    # Phase 5: Facts
    dbt_test_dimensions >> facts_layer_start
    facts_layer_start >> [dbt_run_fact_sales, dbt_run_fact_orders, dbt_run_fact_inventory]
    [dbt_run_fact_sales, dbt_run_fact_orders, dbt_run_fact_inventory] >> facts_complete
    facts_complete >> dbt_test_facts
    
    # Phase 6: Reconciliation
    dbt_test_facts >> reconciliation_layer_start
    reconciliation_layer_start >> dbt_run_reconciliation
    dbt_run_reconciliation >> dbt_test_data_loss
    dbt_test_data_loss >> reconciliation_check
    
    # Phase 7: Analytics
    reconciliation_check >> analytics_layer_start
    analytics_layer_start >> [dbt_run_customer_ltv, dbt_run_product_performance, dbt_run_sales_metrics]
    [dbt_run_customer_ltv, dbt_run_product_performance, dbt_run_sales_metrics] >> analytics_complete
    analytics_complete >> dbt_test_analytics
    
    # Phase 8: Monitoring & Documentation
    dbt_test_analytics >> monitoring_layer_start
    monitoring_layer_start >> [dbt_run_quality_dashboard, dbt_run_quality_trends]
    [dbt_run_quality_dashboard, dbt_run_quality_trends] >> monitoring_complete
    monitoring_complete >> dbt_docs_generate
    dbt_docs_generate >> pipeline_end