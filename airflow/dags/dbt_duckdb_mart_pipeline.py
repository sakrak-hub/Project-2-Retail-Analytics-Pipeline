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

logger = logging.getLogger(__name__)

def run_gold_with_schema_detection(**context):

    ti = context['ti']
    
    cmd = "cd /opt/airflow/dbt && dbt run --select mart --profiles-dir /opt/airflow/dbt"
    
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
        return 'schema_reconcile_gold'
    
    if result.returncode == 0:
        logger.info("✅ Staging run successful")
        ti.xcom_push(key='schema_change_detected', value=False)
        return 'continue_pipeline'

    logger.error(f"Staging run failed: {output}")
    raise ValueError("Staging run failed")

def reconcile_gold_schema(**context):

    logger.info("🔧 Running schema reconciliation (full_refresh)")
    
    cmd = "cd /opt/airflow/dbt && dbt run --select mart --full-refresh --profiles-dir /opt/airflow/dbt"
    
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
        
        # Check 1: Dimension record counts
        dim_counts = conn.execute("""
            SELECT 
                'dim_customers' as dimension,
                COUNT(*) as total_versions,
                COUNT(*) FILTER (WHERE is_current = TRUE) as current_versions
            FROM dim_customers
            UNION ALL
            SELECT 'dim_products', COUNT(*), COUNT(*) FILTER (WHERE is_current = TRUE) FROM dim_products
            UNION ALL
            SELECT 'dim_stores', COUNT(*), COUNT(*) FILTER (WHERE is_current = TRUE) FROM dim_stores
            UNION ALL
            SELECT 'dim_date', COUNT(*), NULL FROM dim_date
        """).fetchdf()
        
        logger.info(f"Dimension counts:\n{dim_counts}")
        
        # Check current dimension counts are reasonable
        customers_current = dim_counts[dim_counts['dimension'] == 'dim_customers']['current_versions'].iloc[0]
        products_current = dim_counts[dim_counts['dimension'] == 'dim_products']['current_versions'].iloc[0]
        stores_current = dim_counts[dim_counts['dimension'] == 'dim_stores']['current_versions'].iloc[0]
        date_count = dim_counts[dim_counts['dimension'] == 'dim_date']['total_versions'].iloc[0]
        
        # Validate minimum counts
        if customers_current < 1000:
            raise ValueError(f"Too few customers: {customers_current} (expected > 1000)")
        if products_current < 100:
            raise ValueError(f"Too few products: {products_current} (expected > 100)")
        if stores_current < 10:
            raise ValueError(f"Too few stores: {stores_current} (expected > 10)")
        if date_count != 4018:
            logger.warning(f"Unexpected date count: {date_count} (expected 4018)")
        
        ti.xcom_push(key='dim_customers_count', value=int(customers_current))
        ti.xcom_push(key='dim_products_count', value=int(products_current))
        ti.xcom_push(key='dim_stores_count', value=int(stores_current))

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
            FROM fact_sales
        """).fetchdf()
        
        logger.info(f"Fact table stats:\n{fact_stats}")
        
        total_sales = fact_stats['total_sales'].iloc[0]
        total_revenue = fact_stats['total_revenue'].iloc[0]
        
        if total_sales < 1000:
            raise ValueError(f"Too few sales records: {total_sales} (expected > 1000)")
        
        ti.xcom_push(key='total_sales', value=int(total_sales))
        ti.xcom_push(key='total_revenue', value=float(total_revenue))

        scd2_check = conn.execute("""
            SELECT 
                'dim_customers' as dimension,
                COUNT(*) as duplicate_current_versions
            FROM (
                SELECT customer_id, COUNT(*) as versions
                FROM dim_customers
                WHERE is_current = TRUE
                GROUP BY customer_id
                HAVING COUNT(*) > 1
            )
            UNION ALL
            SELECT 'dim_products', COUNT(*)
            FROM (
                SELECT product_id, COUNT(*) as versions
                FROM dim_products
                WHERE is_current = TRUE
                GROUP BY product_id
                HAVING COUNT(*) > 1
            )
            UNION ALL
            SELECT 'dim_stores', COUNT(*)
            FROM (
                SELECT store_id, COUNT(*) as versions
                FROM dim_stores
                WHERE is_current = TRUE
                GROUP BY store_id
                HAVING COUNT(*) > 1
            )
        """).fetchdf()
        
        logger.info(f"SCD2 integrity check:\n{scd2_check}")
        
        total_scd2_violations = scd2_check['duplicate_current_versions'].sum()
        if total_scd2_violations > 0:
            logger.error(f"SCD2 integrity violations found: {total_scd2_violations}")
            raise ValueError("SCD2 integrity check failed - duplicate current versions detected")
        
        orphan_check = conn.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE customer_key = MD5('-1')) as orphan_customers,
                COUNT(*) FILTER (WHERE product_key = MD5('-1')) as orphan_products,
                COUNT(*) FILTER (WHERE store_key = MD5('-1')) as orphan_stores
            FROM fact_sales
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
        
        quality_score = 100.0
        if total_scd2_violations > 0:
            quality_score -= 30
        if orphan_customers > 100:
            quality_score -= 10
        if orphan_products > 100:
            quality_score -= 10
        if days_old > 7:
            quality_score -= 5
        
        ti.xcom_push(key='gold_quality_score', value=quality_score)
        
        logger.info(f"✅ Gold layer quality gate PASSED - Score: {quality_score}/100")
        
        return {
            'quality_score': quality_score,
            'total_sales': int(total_sales),
            'total_revenue': float(total_revenue),
            'checks_passed': True
        }
        
    except Exception as e:
        logger.error(f"❌ Gold layer quality gate FAILED: {e}")
    
    finally:
        conn.close()

def generate_gold_metrics(**context):

    ti = context['ti']
    conn = duckdb.connect('/opt/airflow/dbt/warehouse.duckdb')
    
    try:
        logger.info("📊 Generating gold layer metrics...")
        
        # Create metrics table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS retail_transactions_data.gold_metrics_log (
                metric_id INTEGER PRIMARY KEY,
                run_date TIMESTAMP NOT NULL,
                dag_run_id VARCHAR,
                
                -- Dimension metrics
                dim_customers_current INTEGER,
                dim_customers_total INTEGER,
                dim_products_current INTEGER,
                dim_products_total INTEGER,
                dim_stores_current INTEGER,
                dim_stores_total INTEGER,
                
                -- Fact metrics
                total_sales_records INTEGER,
                total_revenue DECIMAL(18,2),
                total_profit DECIMAL(18,2),
                profit_margin_pct DECIMAL(5,2),
                
                -- Data quality
                quality_score DECIMAL(5,2),
                orphan_customers INTEGER,
                orphan_products INTEGER,
                orphan_stores INTEGER,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Get metrics
        dim_metrics = conn.execute("""
            SELECT 
                (SELECT COUNT(*) FROM retail_transactions_data.dim_customers WHERE is_current = TRUE) as customers_current,
                (SELECT COUNT(*) FROM retail_transactions_data.dim_customers) as customers_total,
                (SELECT COUNT(*) FROM retail_transactions_data.dim_products WHERE is_current = TRUE) as products_current,
                (SELECT COUNT(*) FROM retail_transactions_data.dim_products) as products_total,
                (SELECT COUNT(*) FROM retail_transactions_data.dim_stores WHERE is_current = TRUE) as stores_current,
                (SELECT COUNT(*) FROM retail_transactions_data.dim_stores) as stores_total
        """).fetchone()
        
        fact_metrics = conn.execute("""
            SELECT 
                COUNT(*) as total_sales,
                SUM(line_total) as total_revenue,
                SUM(line_profit) as total_profit,
                AVG(line_profit_margin_pct) as avg_margin
            FROM retail_transactions_data.fact_sales
            WHERE is_refund = FALSE
        """).fetchone()
        
        orphan_metrics = conn.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE customer_key = MD5('-1')) as orphan_customers,
                COUNT(*) FILTER (WHERE product_key = MD5('-1')) as orphan_products,
                COUNT(*) FILTER (WHERE store_key = MD5('-1')) as orphan_stores
            FROM retail_transactions_data.fact_sales
        """).fetchone()

        quality_score = ti.xcom_pull(task_ids='gold_quality_gate', key='gold_quality_score')

        next_id = conn.execute("""
            SELECT COALESCE(MAX(metric_id), 0) + 1 
            FROM retail_transactions_data.gold_metrics_log
        """).fetchone()[0]
        
        conn.execute("""
            INSERT INTO retail_transactions_data.gold_metrics_log 
            (metric_id, run_date, dag_run_id,
             dim_customers_current, dim_customers_total,
             dim_products_current, dim_products_total,
             dim_stores_current, dim_stores_total,
             total_sales_records, total_revenue, total_profit, profit_margin_pct,
             quality_score, orphan_customers, orphan_products, orphan_stores)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            next_id,
            context['logical_date'],
            context['dag_run'].run_id,
            dim_metrics[0], dim_metrics[1],
            dim_metrics[2], dim_metrics[3],
            dim_metrics[4], dim_metrics[5],
            fact_metrics[0], fact_metrics[1], fact_metrics[2], fact_metrics[3],
            quality_score,
            orphan_metrics[0], orphan_metrics[1], orphan_metrics[2]
        ])
        
        logger.info("✅ Gold layer metrics logged successfully")

        logger.info(f"""
        📊 GOLD LAYER SUMMARY:
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        Dimensions:
          - Customers: {dim_metrics[0]:,} current ({dim_metrics[1]:,} total)
          - Products:  {dim_metrics[2]:,} current ({dim_metrics[3]:,} total)
          - Stores:    {dim_metrics[4]:,} current ({dim_metrics[5]:,} total)
        
        Facts:
          - Sales Records: {fact_metrics[0]:,}
          - Revenue:       ${fact_metrics[1]:,.2f}
          - Profit:        ${fact_metrics[2]:,.2f}
          - Margin:        {fact_metrics[3]:.2f}%
        
        Quality:
          - Score:         {quality_score}/100
          - Orphans:       {orphan_metrics[0] + orphan_metrics[1] + orphan_metrics[2]}
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """)
    
    except Exception as e:
        logger.error(f"Failed to generate gold metrics: {e}")
        raise
        
    finally:
        conn.close()

def export_business_metrics(**context):

    conn = duckdb.connect('/opt/airflow/dbt/warehouse.duckdb')
    
    try:
        logger.info("📤 Exporting business metrics...")
        
        daily_metrics = conn.execute("""
            SELECT 
                d.date_actual,
                d.year,
                d.month_name,
                d.is_weekend,
                COUNT(DISTINCT f.sales_key) as order_count,
                COUNT(DISTINCT f.customer_key) as unique_customers,
                SUM(f.line_total) as revenue,
                SUM(f.line_profit) as profit,
                AVG(f.line_profit_margin_pct) as avg_margin_pct
            FROM retail_transactions_data.fact_sales f
            JOIN dim_date d ON f.date_key = d.date_key
            WHERE f.is_refund = FALSE
              AND d.is_last_30_days = TRUE
            GROUP BY 1, 2, 3, 4
            ORDER BY 1 DESC
        """).fetchdf()
        
        daily_metrics.to_csv('/opt/airflow/exports/daily_metrics.csv', index=False)
        logger.info(f"✅ Exported {len(daily_metrics)} days of metrics")
        
    except Exception as e:
        logger.warning(f"Failed to export metrics: {e}")
        
    finally:
        conn.close()

with DAG(
    'retail_analytics_dbt_duckdb_mart',
    default_args={
        'owner': 'Sakkaravarthi',
        'depends_on_past': False,
        'email': ['sakra_k@outlook.com'],
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
        bash_command='cd /opt/airflow/dbt && dbt test --select mart --profiles-dir /opt/airflow/dbt',
        trigger_rule='none_failed_min_one_success'
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

    gold_layer_complete = EmptyOperator(
        task_id='gold_layer_complete',
        trigger_rule='all_success'
    )


    gold_layer_start >> dbt_run_gold_with_detection 

    dbt_run_gold_with_detection >> [continue_pipeline, schema_reconcile_gold]

    schema_reconcile_gold >> log_schema_change_task >> dbt_test_gold

    continue_pipeline >> dbt_test_gold

    dbt_test_gold >> gold_quality_gate

    gold_quality_gate >> generate_metrics

    generate_metrics >> [export_metrics, generate_docs]

    [export_metrics, generate_docs] >> gold_layer_complete