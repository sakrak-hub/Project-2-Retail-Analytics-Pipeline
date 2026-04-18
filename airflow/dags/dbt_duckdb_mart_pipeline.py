from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import logging
import duckdb
import subprocess
import pandas as pd
import os

logger = logging.getLogger(__name__)

def dag_failure_callback(context):

    task_instance = context['task_instance']
    exception = context.get('exception')

    logger.info(f"⚠️  Task {task_instance.task_id} failed: {exception}")

    if 'Could not set lock on file' in str(exception):

        SOURCE_DB = "/opt/airflow/dbt/warehouse.duckdb"

        for attempt in range(1, 31):
            try:
                conn = duckdb.connect(SOURCE_DB, read_only=True)
                logger.info(f"✅ Unlocked after {attempt} attempts!")
                conn.close()
                return
            except duckdb.IOException:
                if attempt < 30:
                    time.sleep(10)
                else:
                    logger.error("❌ Timeout")
                    return
    else:
        print(f"Retry {str(task_instance)}!")

def run_gold_with_schema_detection(**context):

    ti = context['ti']
    
    cmd = "cd /opt/airflow/dbt && dbt run --select models/mart/dim --profiles-dir /opt/airflow/dbt && dbt run --select models/mart/fact --profiles-dir /opt/airflow/dbt"
    
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

    no_execution = ('Nothing to do. Try checking your model configs and model specification args' in output)

    completed_execution_successfully = (
        'Finished running' in output and
        'Completed successfully' in output
    )

    if is_schema_change:
        logger.warning("🔄 SCHEMA CHANGE DETECTED - Branching to reconciliation")
        ti.xcom_push(key='schema_change_detected', value=True)
        ti.xcom_push(key='schema_change_time', value=str(context['logical_date']))
        return 'schema_reconcile_gold'
    
    if no_execution:
        raise ValueError('DAG was not executed!')

    if completed_execution_successfully:
        logger.info("✅ Mart models run successful")
        ti.xcom_push(key='schema_change_detected', value=False)
        return 'continue_pipeline'

    logger.error(f"Staging run failed: {output}")
    raise ValueError("Staging run failed")

def reconcile_gold_schema(**context):

    logger.info("🔧 Running schema reconciliation (full_refresh)")
    
    cmd = "cd /opt/airflow/dbt && dbt run --select models/mart/dim --full-refresh --profiles-dir /opt/airflow/dbt && dbt run --select models/mart/fact --full-refresh --profiles-dir /opt/airflow/dbt"
    
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
        task_ids='run_gold_with_detection',
        key='schema_change_detected'
    )
    
    if not schema_change:
        return 
    
    schema_time = ti.xcom_pull(
        task_ids='run_gold_with_detection',
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
        """, [next_id, 'fact_sales', schema_time, 'full_refresh', 'completed', dag_run_id])
        
        logger.info("✅ Schema change logged to database")
    
    except Exception as e:
        logger.error(f"Failed to log schema change: {e}")
        
    finally:
        conn.close()

def check_gold_quality_gate(**context):
    
    ti = context['ti']
    conn = duckdb.connect('/opt/airflow/dbt/warehouse.duckdb')
    
    try:
        logger.info("🔍 Running gold layer quality checks...")

        dim_counts = conn.execute("""
            SELECT 
                'dim_customers' as dimension,
                COUNT(*) as row_count
            FROM mart_db.dim_customers
            UNION ALL
            SELECT 'dim_products', COUNT(*) FROM mart_db.dim_products
            UNION ALL
            SELECT 'dim_stores', COUNT(*) FROM mart_db.dim_stores
            UNION ALL
            SELECT 'dim_date', COUNT(*) FROM mart_db.dim_date
        """).fetchdf()
        
        logger.info(f"Dimension counts:\n{dim_counts}")
        
        customers_count = dim_counts[dim_counts['dimension'] == 'dim_customers']['row_count'].iloc[0]
        products_count = dim_counts[dim_counts['dimension'] == 'dim_products']['row_count'].iloc[0]
        stores_count = dim_counts[dim_counts['dimension'] == 'dim_stores']['row_count'].iloc[0]
        date_count = dim_counts[dim_counts['dimension'] == 'dim_date']['row_count'].iloc[0]
        
        if customers_count < 1000:
            raise ValueError(f"Too few customers: {customers_count} (expected > 1000)")
        if products_count < 100:
            raise ValueError(f"Too few products: {products_count} (expected > 100)")
        if stores_count < 10:
            raise ValueError(f"Too few stores: {stores_count} (expected > 10)")
        if date_count != 4018:
            logger.warning(f"Unexpected date count: {date_count} (expected 4018)")

        ti.xcom_push(key='dim_customers_count', value=int(customers_count))
        ti.xcom_push(key='dim_products_count', value=int(products_count))
        ti.xcom_push(key='dim_stores_count', value=int(stores_count))

        fact_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_sales,
                COUNT(DISTINCT customer_key) as unique_customers,
                COUNT(DISTINCT product_key) as unique_products,
                COUNT(DISTINCT store_key) as unique_stores,
                SUM(line_total) as total_revenue,
                SUM(line_profit) as total_profit,
                MIN(transaction_date) as earliest_date,
                MAX(transaction_date) as latest_date
            FROM mart_db.fact_sales
            WHERE is_refund = FALSE
        """).fetchdf()
        
        logger.info(f"Fact table stats:\n{fact_stats}")
        
        total_sales = fact_stats['total_sales'].iloc[0]
        total_revenue = fact_stats['total_revenue'].iloc[0]
        
        if total_sales < 1000:
            raise ValueError(f"Too few sales records: {total_sales} (expected > 1000)")
        
        ti.xcom_push(key='total_sales', value=int(total_sales))
        ti.xcom_push(key='total_revenue', value=float(total_revenue))

        orphan_check = conn.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE customer_key = MD5('-1')) as orphan_customers,
                COUNT(*) FILTER (WHERE product_key = MD5('-1')) as orphan_products,
                COUNT(*) FILTER (WHERE store_key = MD5('-1')) as orphan_stores
            FROM mart_db.fact_sales
        """).fetchdf()
        
        logger.info(f"Orphan records:\n{orphan_check}")
        
        orphan_customers = orphan_check['orphan_customers'].iloc[0]
        orphan_products = orphan_check['orphan_products'].iloc[0]
        orphan_stores = orphan_check['orphan_stores'].iloc[0]

        if orphan_customers > 100:
            logger.warning(f"High number of orphan customers: {orphan_customers}")
        if orphan_products > 100:
            logger.warning(f"High number of orphan products: {orphan_products}")
        if orphan_stores > 100:
            logger.warning(f"High number of orphan stores: {orphan_stores}")
        
        latest_date = fact_stats['latest_date'].iloc[0]
        days_old = (pd.Timestamp.now() - pd.Timestamp(latest_date)).days
        
        logger.info(f"Latest transaction date: {latest_date} ({days_old} days old)")
        
        if days_old > 7:
            logger.warning(f"Data may be stale: {days_old} days old")
        
        logger.info("🔍 Validating mart views...")
        
        mart_counts = conn.execute("""
            SELECT 
                'revenue_performance' as mart_view,
                COUNT(*) as row_count,
                COUNT(*) FILTER (WHERE revenue IS NULL) as null_revenue_count,
                COUNT(*) FILTER (WHERE transactions IS NULL) as null_transactions_count
            FROM aggregated_db.revenue_performance
            UNION ALL
            SELECT 
                'store_performance',
                COUNT(*),
                COUNT(*) FILTER (WHERE total_revenue IS NULL),
                COUNT(*) FILTER (WHERE total_transactions IS NULL)
            FROM aggregated_db.store_performance
            UNION ALL
            SELECT 
                'products_performance',
                COUNT(*),
                COUNT(*) FILTER (WHERE total_revenue IS NULL),
                COUNT(*) FILTER (WHERE revenue_rank IS NULL)
            FROM aggregated_db.products_performance
            UNION ALL
            SELECT 
                'customer_metrics',
                COUNT(*),
                COUNT(*) FILTER (WHERE lifetime_revenue IS NULL),
                COUNT(*) FILTER (WHERE total_purchases IS NULL)
            FROM aggregated_db.customer_metrics
        """).fetchdf()
        
        logger.info(f"Mart view counts:\n{mart_counts}")
        
        for idx, row in mart_counts.iterrows():
            mart_name = row['mart_view']
            row_count = row['row_count']
            null_count = row['null_revenue_count'] + row['null_transactions_count']
            
            if row_count == 0:
                raise ValueError(f"Mart view {mart_name} is empty!")
            
            if null_count > 0:
                logger.warning(f"Mart view {mart_name} has {null_count} null values in critical fields")
        
        revenue_performance_count = mart_counts[mart_counts['mart_view'] == 'revenue_performance']['row_count'].iloc[0]
        store_performance_count = mart_counts[mart_counts['mart_view'] == 'store_performance']['row_count'].iloc[0]
        products_performance_count = mart_counts[mart_counts['mart_view'] == 'products_performance']['row_count'].iloc[0]
        customer_metrics_count = mart_counts[mart_counts['mart_view'] == 'customer_metrics']['row_count'].iloc[0]
        
        if revenue_performance_count < 30:
            logger.warning(f"revenue_performance has only {revenue_performance_count} days (expected > 30)")

        if abs(store_performance_count - stores_count) > 1:
            logger.warning(f"store_performance count ({store_performance_count}) doesn't match dim_stores ({stores_count})")
        
        if abs(products_performance_count - products_count) > 10:
            logger.warning(f"products_performance count ({products_performance_count}) doesn't match dim_products ({products_count})")
        
        if abs(customer_metrics_count - customers_count) > 10:
            logger.warning(f"customer_metrics count ({customer_metrics_count}) doesn't match dim_customers ({customers_count})")
        
        ti.xcom_push(key='revenue_performance_count', value=int(revenue_performance_count))
        ti.xcom_push(key='store_performance_count', value=int(store_performance_count))
        ti.xcom_push(key='products_performance_count', value=int(products_performance_count))
        ti.xcom_push(key='customer_metrics_count', value=int(customer_metrics_count))
        
        logger.info("🔍 Reconciling mart views with fact table...")
        
        reconciliation = conn.execute("""
            SELECT 
                -- Fact table totals
                (SELECT SUM(line_total) FROM mart_db.fact_sales WHERE is_refund = FALSE) as fact_revenue,
                (SELECT COUNT(DISTINCT transaction_id) FROM mart_db.fact_sales WHERE is_refund = FALSE) as fact_transactions,
                
                -- Mart totals
                (SELECT SUM(revenue) FROM aggregated_db.revenue_performance) as revenue_mart_total,
                (SELECT SUM(total_revenue) FROM aggregated_db.store_performance) as store_mart_total,
                (SELECT SUM(total_revenue) FROM aggregated_db.products_performance) as product_mart_total,
                (SELECT SUM(lifetime_revenue) FROM aggregated_db.customer_metrics) as customer_mart_total
        """).fetchdf()
        
        logger.info(f"Reconciliation check:\n{reconciliation}")
        
        fact_revenue = reconciliation['fact_revenue'].iloc[0]
        revenue_mart_total = reconciliation['revenue_mart_total'].iloc[0]
        store_mart_total = reconciliation['store_mart_total'].iloc[0]
        product_mart_total = reconciliation['product_mart_total'].iloc[0]
        customer_mart_total = reconciliation['customer_mart_total'].iloc[0]
        
        revenue_tolerance = fact_revenue * 0.01
        
        if abs(revenue_mart_total - fact_revenue) > revenue_tolerance:
            logger.warning(f"revenue_performance total ({revenue_mart_total:.2f}) doesn't match fact_sales ({fact_revenue:.2f})")
        
        if abs(store_mart_total - fact_revenue) > revenue_tolerance:
            logger.warning(f"store_performance total ({store_mart_total:.2f}) doesn't match fact_sales ({fact_revenue:.2f})")
        
        if abs(product_mart_total - fact_revenue) > revenue_tolerance:
            logger.warning(f"products_performance total ({product_mart_total:.2f}) doesn't match fact_sales ({fact_revenue:.2f})")
        
        if abs(customer_mart_total - fact_revenue) > revenue_tolerance:
            logger.warning(f"customer_metrics total ({customer_mart_total:.2f}) doesn't match fact_sales ({fact_revenue:.2f})")
        
        quality_score = 100.0
        
        if orphan_customers > 100:
            quality_score -= 10
        if orphan_products > 100:
            quality_score -= 10
        if orphan_stores > 100:
            quality_score -= 10
        if days_old > 7:
            quality_score -= 5
        
        if revenue_performance_count < 30:
            quality_score -= 5
        if abs(revenue_mart_total - fact_revenue) > revenue_tolerance:
            quality_score -= 5
        if abs(store_mart_total - fact_revenue) > revenue_tolerance:
            quality_score -= 5
        
        ti.xcom_push(key='gold_quality_score', value=quality_score)
        
        logger.info(f"""
        ✅ GOLD LAYER QUALITY GATE PASSED
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        DIMENSIONS:
          • Customers:  {customers_count:,}
          • Products:   {products_count:,}
          • Stores:     {stores_count:,}
          • Dates:      {date_count:,}
        
        FACTS:
          • Sales:      {total_sales:,} records
          • Revenue:    ${total_revenue:,.2f}
          • Freshness:  {days_old} days old
        
        MART VIEWS:
          • Revenue Performance:   {revenue_performance_count:,} days
          • Store Performance:     {store_performance_count:,} stores
          • Products Performance:  {products_performance_count:,} products
          • Customer Metrics:      {customer_metrics_count:,} customers
        
        DATA QUALITY:
          • Orphan Records:   {orphan_customers + orphan_products + orphan_stores}
          • Quality Score:    {quality_score:.1f}/100
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """)
        
        return {
            'quality_score': quality_score,
            'total_sales': int(total_sales),
            'total_revenue': float(total_revenue),
            'views_validated': True,
            'checks_passed': True
        }
        
    except Exception as e:
        logger.error(f"❌ Gold layer quality gate FAILED: {e}")
        raise
    
    finally:
        conn.close()

def generate_gold_metrics(**context):
    
    ti = context['ti']
    conn = duckdb.connect('/opt/airflow/dbt/warehouse.duckdb')
    
    try:
        logger.info("📊 Generating gold layer metrics...")
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS aggregated_db.gold_metrics_log (
                metric_id INTEGER PRIMARY KEY,
                run_date TIMESTAMP NOT NULL,
                dag_run_id VARCHAR,
                
                -- Dimension metrics
                dim_customers_count INTEGER,
                dim_products_count INTEGER,
                dim_stores_count INTEGER,
                dim_date_count INTEGER,
                
                -- Fact metrics
                total_sales_records INTEGER,
                total_revenue DECIMAL(18,2),
                total_profit DECIMAL(18,2),
                profit_margin_pct DECIMAL(5,2),
                
                -- Mart metrics (new)
                revenue_performance_days INTEGER,
                store_performance_stores INTEGER,
                products_performance_products INTEGER,
                customer_metrics_customers INTEGER,
                
                -- Data quality
                quality_score DECIMAL(5,2),
                orphan_customers INTEGER,
                orphan_products INTEGER,
                orphan_stores INTEGER,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        dim_metrics = conn.execute("""
            SELECT 
                (SELECT COUNT(*) FROM mart_db.dim_customers) as customers_count,
                (SELECT COUNT(*) FROM mart_db.dim_products) as products_count,
                (SELECT COUNT(*) FROM mart_db.dim_stores) as stores_count,
                (SELECT COUNT(*) FROM mart_db.dim_date) as date_count
        """).fetchone()
        
        fact_metrics = conn.execute("""
            SELECT 
                COUNT(*) as total_sales,
                SUM(line_total) as total_revenue,
                SUM(line_profit) as total_profit,
                AVG(line_profit_margin_pct) as avg_margin
            FROM mart_db.fact_sales
            WHERE is_refund = FALSE
        """).fetchone()
        
        mart_metrics = conn.execute("""
            SELECT 
                (SELECT COUNT(*) FROM aggregated_db.revenue_performance) as revenue_days,
                (SELECT COUNT(*) FROM aggregated_db.store_performance) as store_count,
                (SELECT COUNT(*) FROM aggregated_db.products_performance) as product_count,
                (SELECT COUNT(*) FROM aggregated_db.customer_metrics) as customer_count
        """).fetchone()
        
        orphan_metrics = conn.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE customer_key = MD5('-1')) as orphan_customers,
                COUNT(*) FILTER (WHERE product_key = MD5('-1')) as orphan_products,
                COUNT(*) FILTER (WHERE store_key = MD5('-1')) as orphan_stores
            FROM mart_db.fact_sales
        """).fetchone()

        quality_score = ti.xcom_pull(task_ids='gold_quality_gate', key='gold_quality_score')

        next_id = conn.execute("""
            SELECT COALESCE(MAX(metric_id), 0) + 1 
            FROM aggregated_db.gold_metrics_log
        """).fetchone()[0]

        conn.execute("""
            INSERT INTO aggregated_db.gold_metrics_log 
            (metric_id, run_date, dag_run_id,
             dim_customers_count, dim_products_count, dim_stores_count, dim_date_count,
             total_sales_records, total_revenue, total_profit, profit_margin_pct,
             revenue_performance_days, store_performance_stores, 
             products_performance_products, customer_metrics_customers,
             quality_score, orphan_customers, orphan_products, orphan_stores)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            next_id,
            context['logical_date'],
            context['dag_run'].run_id,
            dim_metrics[0], dim_metrics[1], dim_metrics[2], dim_metrics[3],
            fact_metrics[0], fact_metrics[1], fact_metrics[2], fact_metrics[3],
            mart_metrics[0], mart_metrics[1], mart_metrics[2], mart_metrics[3],
            quality_score,
            orphan_metrics[0], orphan_metrics[1], orphan_metrics[2]
        ])
        
        logger.info("✅ Gold layer metrics logged successfully")

        logger.info(f"""
        📊 GOLD LAYER SUMMARY:
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        Dimensions (Static):
          - Customers: {dim_metrics[0]:,}
          - Products:  {dim_metrics[1]:,}
          - Stores:    {dim_metrics[2]:,}
          - Dates:     {dim_metrics[3]:,}
        
        Facts:
          - Sales Records: {fact_metrics[0]:,}
          - Revenue:       ${fact_metrics[1]:,.2f}
          - Profit:        ${fact_metrics[2]:,.2f}
          - Margin:        {fact_metrics[3]:.2f}%
        
        Marts:
          - Revenue Performance: {mart_metrics[0]:,} days
          - Store Performance:   {mart_metrics[1]:,} stores
          - Products Performance: {mart_metrics[2]:,} products
          - Customer Metrics:    {mart_metrics[3]:,} customers
        
        Quality:
          - Score:   {quality_score:.1f}/100
          - Orphans: {orphan_metrics[0] + orphan_metrics[1] + orphan_metrics[2]}
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """)
    
    except Exception as e:
        logger.error(f"Failed to generate gold metrics: {e}")
        raise
        
    finally:
        conn.close()

def export_business_metrics(**context):
    """
    Export business metrics to CSV for external reporting.
    
    Uses revenue_performance mart (simplified approach - no fact table queries)
    """
    
    conn = duckdb.connect('/opt/airflow/dbt/warehouse.duckdb')
    
    try:
        logger.info("📤 Exporting business metrics...")
        
        daily_metrics = conn.execute("""
            SELECT 
                date,
                year,
                quarter,
                month,
                month_name,
                day_name,
                day_type,
                revenue,
                cost,
                profit,
                profit_margin_pct,
                transactions,
                customers as unique_customers,
                units_sold,
                avg_order_value,
                revenue_7day_avg,
                revenue_30day_avg,
                revenue_mtd,
                revenue_ytd
            FROM aggregated_db.revenue_performance
            WHERE is_last_30_days = TRUE
            ORDER BY date DESC
        """).fetchdf()
        
        daily_metrics.to_csv('/opt/airflow/master_data/daily_metrics.csv', index=False)
        
        logger.info(f"✅ Exported {len(daily_metrics)} days of metrics")
        logger.info(f"   Revenue range: ${daily_metrics['revenue'].min():,.2f} - ${daily_metrics['revenue'].max():,.2f}")
        logger.info(f"   Total revenue (30d): ${daily_metrics['revenue'].sum():,.2f}")
        
    except Exception as e:
        logger.error(f"❌ Failed to export business metrics: {e}")
        raise ValueError(f"Error exporting metrics: {e}")
        
    finally:
        conn.close()

def sync_bi_database(**context):
    
    SOURCE_DB = "/opt/airflow/dbt/warehouse.duckdb"
    TARGET_DB = "/opt/airflow/dbt/warehouse_bi.duckdb"
    
    logger.info("🔄 Syncing to BI database...")
    
    try:
        if os.path.exists(TARGET_DB):
            os.remove(TARGET_DB)
        
        target_conn = duckdb.connect(TARGET_DB)

        target_conn.execute(f"ATTACH '{SOURCE_DB}' AS source_db (READ_ONLY)")
        target_conn.execute("""
        CREATE SCHEMA IF NOT EXISTS mart_db;
        CREATE SCHEMA IF NOT EXISTS aggregated_db;
        """)
        
        tables = [
            'mart_db.dim_customers', 'mart_db.dim_products', 'mart_db.dim_stores', 'mart_db.dim_date',
            'mart_db.fact_sales',
            'aggregated_db.customer_metrics',
            'aggregated_db.gold_metrics_log', 'aggregated_db.products_performance', 'aggregated_db.revenue_performance',
            'aggregated_db.store_performance'
        ]

        for table in tables:
            logger.info(f"   Syncing {table}...")
            target_conn.execute(f"CREATE TABLE {table} AS SELECT * FROM source_db.{table}")
        
        target_conn.execute("""
            CREATE TABLE _sync_metadata AS
            SELECT 
                CURRENT_TIMESTAMP as last_sync_time,
                'success' as status
        """)
        
        target_conn.execute("DETACH source_db")
        
        logger.info("✅ BI database synced successfully")
        
    except Exception as e:
        logger.error(f"❌ BI sync failed: {e}")
        raise
    
    finally:
        target_conn.close()

def update_motherduck_db(**context):

    source_db_path = '/opt/airflow/dbt/warehouse_bi.duckdb'

    queries_list = [
        "DETACH DATABASE IF EXISTS warehouse_bi",
        "DROP DATABASE IF EXISTS warehouse_bi",
        "CREATE OR REPLACE DATABASE warehouse_bi FROM '/opt/airflow/dbt/warehouse_bi.duckdb'"]

    
    try:
        md_con = duckdb.connect('md:')

        for query in queries_list:
            md_con.execute(query)

        df = md_con.execute("SELECT * FROM duckdb_tables() WHERE database_name='warehouse_bi';").fetchdf()

        if len(df)==0:
            raise ValueError('Database not created. Please check source!')
    
    except Exception as e:
        logger.error(f"Visualisation DB Update failed!: {e}")
        raise

    finally: 
        md_con.close()

with DAG(
    'retail_analytics_dbt_duckdb_mart',
    default_args={
        'owner': 'Sakkaravarthi',
        'depends_on_past': False,
        'email': ['sakra_k@outlook.com'],
        'on_failure_callback': dag_failure_callback,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='Processing intermediate data for presentation',
    schedule=None, 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['retail', 'dbt', 'quality-gates', 'incremental', 'gold']
) as dag:

    gold_layer_start = EmptyOperator(task_id='gold_layer_start')

    dbt_run_gold_with_detection = BranchPythonOperator(
        task_id='run_gold_with_detection',
        python_callable=run_gold_with_schema_detection
    )

    dbt_run_mart_views = BashOperator(
    task_id='dbt_run_mart_views',
    bash_command='cd /opt/airflow/dbt && dbt run --select models/mart/view --profiles-dir /opt/airflow/dbt'
    )

    continue_pipeline = EmptyOperator(
        task_id='continue_pipeline'
    )

    schema_reconcile_gold = PythonOperator(
        task_id='schema_reconcile_gold',
        python_callable=reconcile_gold_schema
    )

    log_schema_change_task = PythonOperator(
        task_id='log_schema_change',
        python_callable=log_schema_change
    )

    dbt_test_gold = BashOperator( 
        task_id='dbt_test_gold',
        bash_command='cd /opt/airflow/dbt && dbt test --select models/mart/dim --profiles-dir /opt/airflow/dbt && dbt test --select models/mart/fact --profiles-dir /opt/airflow/dbt',
        trigger_rule='none_failed_min_one_success'
    )

    dbt_test_mart_views = BashOperator(
    task_id='dbt_test_mart_views',
    bash_command='cd /opt/airflow/dbt && dbt test --select models/mart/view --profiles-dir /opt/airflow/dbt'
    )

    gold_quality_gate = PythonOperator(
        task_id='gold_quality_gate',
        python_callable=check_gold_quality_gate
    )

    generate_metrics = PythonOperator(
        task_id='generate_gold_metrics',
        python_callable=generate_gold_metrics
    )

    export_metrics = PythonOperator(
        task_id='export_business_metrics',
        python_callable=export_business_metrics,
        trigger_rule='all_done' 
    )

    generate_docs = BashOperator(
        task_id='generate_dbt_docs',
        bash_command='cd /opt/airflow/dbt && dbt docs generate --profiles-dir /opt/airflow/dbt',
        trigger_rule='all_success'
    )

    create_sync_bi_data= PythonOperator(
        task_id='create_sync_bi_data',
        python_callable=sync_bi_database
    )

    update_motherduck_db = PythonOperator(
        task_id = 'update_motherduck_db',
        python_callable=update_motherduck_db
    )
    gold_layer_complete = EmptyOperator(
        task_id='gold_layer_complete',
        trigger_rule='all_success'
    )


    gold_layer_start >> dbt_run_gold_with_detection 

    dbt_run_gold_with_detection >> [continue_pipeline, schema_reconcile_gold]

    schema_reconcile_gold >> log_schema_change_task >> dbt_test_gold

    continue_pipeline >> dbt_test_gold

    dbt_test_gold >> dbt_run_mart_views >> dbt_test_mart_views >> gold_quality_gate

    gold_quality_gate >> generate_metrics

    generate_metrics >> [export_metrics, generate_docs] >> create_sync_bi_data >> update_motherduck_db

    update_motherduck_db >> gold_layer_complete