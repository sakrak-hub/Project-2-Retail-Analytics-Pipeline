{{
    config(
        materialized='incremental',
        unique_key='profile_date',
        tags=['staging', 'quality', 'profile']
    )
}}

WITH revenue_summed AS(
    SELECT
        *,
        CASE 
                WHEN ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY product_id) = 1 
                THEN total_amount 
                ELSE 0 
        END as deduped_revenue
    
    FROM {{ ref('stg_transactions') }}
),

staging_daily AS (
    SELECT 
        DATE_TRUNC('day', staging_loaded_at) AS profile_date,
        
        COUNT(*) AS total_line_items,
        COUNT(DISTINCT line_item_key) AS unique_line_items,
        COUNT(DISTINCT transaction_id) AS unique_transactions,
        COUNT(DISTINCT customer_id) AS unique_customers,
        COUNT(DISTINCT product_id) AS unique_products_sold,
        COUNT(DISTINCT store_id) AS unique_stores,
        
        ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT transaction_id), 2) AS avg_items_per_transaction,
        
        SUM(CASE WHEN data_quality_status = 'CLEAN' THEN 1 ELSE 0 END) AS clean_records,
        SUM(CASE WHEN data_quality_status = 'CORRECTED' THEN 1 ELSE 0 END) AS corrected_records,
        SUM(CASE WHEN data_quality_status = 'INCOMPLETE' THEN 1 ELSE 0 END) AS incomplete_records,
        
        ROUND(SUM(CASE WHEN data_quality_status = 'CLEAN' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS clean_pct,
        ROUND(SUM(CASE WHEN data_quality_status = 'CORRECTED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS corrected_pct,

        SUM(line_total_clean) AS total_line_item_revenue,
        AVG(line_total_clean) AS avg_line_item_value,
        SUM(quantity_clean) AS total_quantity_sold,
        AVG(quantity_clean) AS avg_quantity_per_line,
        
        SUM(deduped_revenue) AS total_transaction_revenue,
        
        SUM(CASE WHEN is_refund THEN 1 ELSE 0 END) AS refund_line_items,
        SUM(CASE WHEN has_promotion THEN 1 ELSE 0 END) AS promotion_line_items,
        SUM(CASE WHEN has_discount THEN 1 ELSE 0 END) AS discount_line_items,
        SUM(CASE WHEN is_bulk_purchase THEN 1 ELSE 0 END) AS bulk_purchase_line_items,
        SUM(CASE WHEN is_high_value_item THEN 1 ELSE 0 END) AS high_value_line_items,
        
        ROUND(SUM(CASE WHEN is_refund THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS refund_pct,
        ROUND(SUM(CASE WHEN has_promotion THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS promotion_pct,
        
        SUM(CASE WHEN transaction_type = 'Regular Sale' THEN 1 ELSE 0 END) AS regular_sale_items,
        SUM(CASE WHEN transaction_type = 'Discounted' THEN 1 ELSE 0 END) AS discounted_items,
        SUM(CASE WHEN transaction_type LIKE '%Promotion%' THEN 1 ELSE 0 END) AS promotional_items,
        SUM(CASE WHEN transaction_type = 'Refund' THEN 1 ELSE 0 END) AS refund_items,
        
        SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS weekend_transactions,
        ROUND(SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS weekend_pct,
        
        SUM(CASE WHEN time_of_day LIKE '%Morning%' THEN 1 ELSE 0 END) AS morning_transactions,
        SUM(CASE WHEN time_of_day LIKE '%Afternoon%' THEN 1 ELSE 0 END) AS afternoon_transactions,
        SUM(CASE WHEN time_of_day LIKE '%Evening%' THEN 1 ELSE 0 END) AS evening_transactions,
        
        AVG(discount_percent) AS avg_discount_percent,
        SUM(discount_amount) AS total_discount_amount,
        
        COUNT(DISTINCT payment_method) AS unique_payment_methods,
        
        SUM(CASE WHEN earned_loyalty_points THEN 1 ELSE 0 END) AS transactions_with_loyalty_points,
        SUM(loyalty_points_earned) AS total_loyalty_points_earned,
        
        GREATEST(0, 
            100 
            - (SUM(CASE WHEN data_quality_status = 'CORRECTED' THEN 1 ELSE 0 END) * 5.0 / COUNT(*))
            - (SUM(CASE WHEN data_quality_status = 'INCOMPLETE' THEN 1 ELSE 0 END) * 10.0 / COUNT(*))
            - (SUM(CASE WHEN is_refund THEN 1 ELSE 0 END) * 2.0 / COUNT(*))
        ) AS transaction_staging_quality_score
        
    FROM revenue_summed
    
    {% if is_incremental() %}
    WHERE DATE_TRUNC('day', staging_loaded_at) > (SELECT MAX(profile_date) FROM {{ this }})
    {% endif %}
    
    GROUP BY DATE_TRUNC('day', staging_loaded_at)
),

profile_enriched AS (
    SELECT 
        *,
        
        CASE 
            WHEN total_line_items = unique_line_items THEN 'Perfect - No Duplicates'
            WHEN total_line_items > unique_line_items THEN 'WARNING - Duplicates Found'
            ELSE 'Unknown'
        END AS deduplication_status,
        
        CASE 
            WHEN avg_items_per_transaction BETWEEN 1.5 AND 3.0 THEN 'Healthy - Normal Distribution'
            WHEN avg_items_per_transaction < 1.5 THEN 'Low - Few Multi-Item Transactions'
            ELSE 'High - Many Multi-Item Transactions'
        END AS transaction_size_health,
        
        CASE 
            WHEN transaction_staging_quality_score >= 98 THEN 'Excellent'
            WHEN transaction_staging_quality_score >= 95 THEN 'Good'
            WHEN transaction_staging_quality_score >= 90 THEN 'Fair'
            ELSE 'Needs Review'
        END AS health_status,
        
        CASE 
            WHEN clean_pct >= 95 THEN 'HIGH_QUALITY'
            WHEN clean_pct >= 90 THEN 'GOOD_QUALITY'
            WHEN clean_pct >= 85 THEN 'ACCEPTABLE_QUALITY'
            ELSE 'NEEDS_IMPROVEMENT'
        END AS quality_tier,
        
        CASE 
            WHEN refund_pct > 10 THEN 'HIGH_REFUNDS'
            WHEN refund_pct > 5 THEN 'ELEVATED_REFUNDS'
            ELSE 'NORMAL_REFUNDS'
        END AS refund_health,
        
        CASE 
            WHEN promotion_pct > 40 THEN 'HEAVY_PROMOTIONS'
            WHEN promotion_pct > 20 THEN 'MODERATE_PROMOTIONS'
            ELSE 'LOW_PROMOTIONS'
        END AS promotion_intensity,
        
        CASE 
            WHEN transaction_staging_quality_score < 90 THEN TRUE
            WHEN clean_pct < 90 THEN TRUE
            WHEN refund_pct > 10 THEN TRUE
            WHEN total_line_items < 1000 THEN TRUE  -- Unusually low volume
            ELSE FALSE
        END AS quality_alert_flag,
        
        CASE 
            WHEN total_line_items = 0 THEN NULL
            ELSE ROUND((unique_line_items * 100.0 / total_line_items), 2)
        END AS deduplication_success_pct,
        
        CURRENT_TIMESTAMP AS profile_created_at,
        'transaction' AS entity_type,
        'line_item' AS grain_level
        
    FROM staging_daily
)

SELECT * FROM profile_enriched
ORDER BY profile_date DESC