{{
    config(
        materialized='incremental',
        unique_key='profile_date',
        tags=['bronze', 'quality', 'profile']
    )
}}

-- Step 1: Add ALL window functions here
WITH line_items_with_duplicate_flag AS (
    SELECT 
        *,
        -- Window function 1: Count occurrences of (txn_id, product_id)
        COUNT(*) OVER (
            PARTITION BY transaction_id_clean, product_id
        ) AS occurrences,
        
        -- Window function 2: Number rows within each transaction
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id_clean 
            ORDER BY product_id
        ) AS transaction_line_number
        
    FROM {{ ref('bronze_transactions') }}
    
    {% if is_incremental() %}
    WHERE DATE_TRUNC('day', _loaded_at) > (SELECT MAX(profile_date) FROM {{ this }})
    {% endif %}
),

-- Step 2: Aggregate with the flag
transactions_daily AS (
    SELECT 
        DATE_TRUNC('day', _loaded_at) AS profile_date,
        COUNT(*) AS total_line_items,
        COUNT(DISTINCT CONCAT(transaction_id_clean, '|', product_id)) AS unique_txn_product_combinations,
        COUNT(DISTINCT transaction_id_clean) AS unique_transactions,
        COUNT(DISTINCT customer_id) AS unique_customers,
        COUNT(DISTINCT product_id) AS unique_products_sold,
        COUNT(DISTINCT store_id) AS unique_stores,

        ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT transaction_id_clean), 2) AS avg_line_items_per_transaction,

        -- ========================================
        -- DUPLICATE DETECTION (NOW WORKS!)
        -- ========================================
        -- Count line items where (transaction_id, product_id) appears more than once
        SUM(
            CASE 
                WHEN occurrences > 1 THEN 1 
                ELSE 0 
            END
        ) AS duplicate_txn_product_line_items,

        -- ========================================
        -- COMPLETENESS FLAGS
        -- ========================================
        SUM(missing_transaction_id_flag) AS missing_transaction_id_clean_count,
        SUM(missing_date_flag) AS missing_date_count,
        SUM(missing_customer_id_flag) AS missing_customer_id_count,
        SUM(missing_product_id_flag) AS missing_product_id_count,

        ROUND(SUM(missing_transaction_id_flag) * 100.0 / COUNT(*), 2) AS missing_txn_id_pct,
        ROUND(SUM(missing_date_flag) * 100.0 / COUNT(*), 2) AS missing_date_pct,
        ROUND(SUM(missing_customer_id_flag) * 100.0 / COUNT(*), 2) AS missing_customer_pct,
        ROUND(SUM(missing_product_id_flag) * 100.0 / COUNT(*), 2) AS missing_product_pct,

        -- ========================================
        -- QUALITY FLAGS
        -- ========================================
        SUM(negative_quantity_flag) AS negative_quantity_count,
        SUM(negative_amount_flag) AS negative_amount_count,
        SUM(zero_total_amount_flag) AS zero_total_count,
        SUM(zero_quantity_flag) AS zero_quantity_count,
        SUM(high_unit_price_flag) AS high_unit_price_count,

        ROUND(SUM(negative_quantity_flag) * 100.0 / COUNT(*), 2) AS negative_quantity_pct,
        ROUND(SUM(zero_total_amount_flag) * 100.0 / COUNT(*), 2) AS zero_total_pct,

        -- ========================================
        -- BUSINESS METRICS - LINE ITEM LEVEL
        -- ========================================
        SUM(line_total) AS total_line_item_revenue,
        AVG(line_total) AS avg_line_item_value,
        SUM(quantity) AS total_quantity_sold,
        AVG(quantity) AS avg_quantity_per_line,

        -- ========================================
        -- BUSINESS METRICS - TRANSACTION LEVEL
        -- ========================================
        -- Transaction revenue (deduplicated using pre-calculated line number)
        SUM(
            CASE 
                WHEN transaction_line_number = 1 
                THEN total_amount 
                ELSE 0 
            END
        ) AS total_transaction_revenue,

        -- ========================================
        -- REFUNDS & PROMOTIONS
        -- ========================================
        SUM(CASE WHEN is_refund = TRUE THEN 1 ELSE 0 END) AS refund_line_items,
        SUM(CASE WHEN has_promotion = TRUE THEN 1 ELSE 0 END) AS promotion_line_items,
        SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END) AS negative_quantity_lines,

        -- ========================================
        -- LINE ITEM QUALITY SCORE
        -- ========================================
        GREATEST(0, 
            100 
            - (SUM(missing_transaction_id_flag) * 100.0 / COUNT(*))  
            - (SUM(missing_date_flag) * 100.0 / COUNT(*))            
            - (SUM(missing_product_id_flag) * 100.0 / COUNT(*))      
            - (SUM(negative_quantity_flag) * 100.0 / COUNT(*) * 0.5) 
            - (SUM(zero_total_amount_flag) * 100.0 / COUNT(*) * 0.5)
        ) AS line_item_quality_score
    
    FROM line_items_with_duplicate_flag
    
    GROUP BY DATE_TRUNC('day', _loaded_at)
),

profile_enriched AS (
    SELECT 
        *,

        -- ========================================
        -- GRAIN HEALTH INDICATORS
        -- ========================================
        CASE 
            WHEN total_line_items = unique_txn_product_combinations THEN 'Clean - No Duplicates'
            WHEN duplicate_txn_product_line_items = 0 THEN 'Clean - No Duplicates'
            WHEN duplicate_txn_product_line_items < total_line_items * 0.01 THEN 'Good - <1% Duplicates'
            WHEN duplicate_txn_product_line_items < total_line_items * 0.05 THEN 'Fair - <5% Duplicates'
            ELSE 'Poor - 5%+ Duplicates'
        END AS line_item_grain_health,

        CASE 
            WHEN avg_line_items_per_transaction BETWEEN 1.5 AND 3.0 THEN 'Healthy - Normal Distribution'
            WHEN avg_line_items_per_transaction < 1.5 THEN 'Low - Few Multi-Item Transactions'
            ELSE 'High - Many Multi-Item Transactions'
        END AS transaction_size_health,

        -- ========================================
        -- OVERALL HEALTH STATUS
        -- ========================================
        CASE 
            WHEN line_item_quality_score >= 95 THEN 'Excellent'
            WHEN line_item_quality_score >= 90 THEN 'Good'
            WHEN line_item_quality_score >= 80 THEN 'Fair'
            WHEN line_item_quality_score >= 70 THEN 'Poor'
            ELSE 'Critical'
        END AS health_status,

        -- ========================================
        -- QUALITY TIER
        -- ========================================
        CASE 
            WHEN missing_txn_id_pct = 0 
                AND missing_date_pct = 0 
                AND missing_product_pct = 0
                AND negative_quantity_pct < 1 
            THEN 'CLEAN'
            WHEN missing_txn_id_pct < 5 
                AND missing_date_pct < 5 
                AND missing_product_pct < 5
                AND negative_quantity_pct < 5 
            THEN 'ACCEPTABLE'
            ELSE 'POOR'
        END AS quality_tier,

        -- ========================================
        -- ALERT FLAGS
        -- ========================================
        CASE 
            WHEN line_item_quality_score < 70 THEN TRUE
            WHEN missing_txn_id_pct > 10 THEN TRUE
            WHEN missing_date_pct > 10 THEN TRUE
            WHEN missing_product_pct > 10 THEN TRUE 
            WHEN duplicate_txn_product_line_items > total_line_items * 0.1 THEN TRUE 
            ELSE FALSE
        END AS quality_alert_flag,

        -- ========================================
        -- GRAIN VALIDATION METRICS
        -- ========================================
        CASE 
            WHEN total_line_items = 0 THEN NULL
            WHEN unique_transactions = 0 THEN NULL
            ELSE ROUND(total_line_items * 1.0 / unique_transactions, 2)
        END AS actual_line_items_per_txn_ratio,

        CASE 
            WHEN total_line_items = 0 THEN NULL
            ELSE ROUND(duplicate_txn_product_line_items * 100.0 / total_line_items, 2)
        END AS duplicate_line_item_pct,

        -- ========================================
        -- METADATA
        -- ========================================
        CURRENT_TIMESTAMP AS profile_created_at,
        '{{ var("source_system", "RETAIL_S3") }}' AS source_system,
        'line_item' AS grain_level
        
    FROM transactions_daily
)

SELECT * FROM profile_enriched
ORDER BY profile_date DESC