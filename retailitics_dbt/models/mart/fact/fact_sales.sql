{{
    config(
        materialized='incremental',
        unique_key='sales_key',
        on_schema_change='fail',
        tags=['gold', 'fact']
    )
}}


WITH sales_transactions AS (
    SELECT * 
    FROM {{ ref('stg_transactions') }}
    
    {% if is_incremental() %}
    WHERE staging_loaded_at > (SELECT MAX(fact_loaded_at) FROM {{ this }})
    {% endif %}
),

sales_with_dimension AS (
    SELECT

        txn.line_item_key AS sales_key,

        CAST(REPLACE(CAST(txn.transaction_date AS VARCHAR), '-', '') AS INTEGER) AS date_key,
        
        COALESCE(cust.customer_key, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) AS customer_key,
        COALESCE(prod.product_key, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) AS product_key,
        COALESCE(store.store_key, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) AS store_key,

        txn.transaction_id,
        txn.cashier_id,
        txn.promotion_code,
        txn.refund_reason,
        txn.store_name,

        txn.transaction_date,
        txn.transaction_time,
        txn.transaction_datetime,
        txn.transaction_year,
        txn.transaction_month,
        txn.transaction_day,
        txn.transaction_hour,

        txn.payment_method,
        txn.payment_status,
        txn.transaction_type,

        txn.is_refund,
        txn.has_promotion,
        txn.has_discount,
        txn.is_weekend,
        txn.is_multi_quantity,
        txn.is_bulk_purchase,
        txn.is_high_value_item,
        txn.earned_loyalty_points,

        txn.day_name,
        txn.time_of_day,

        txn.product_name,
        txn.category,

        txn.quantity_clean AS quantity,
        txn.items_count,

        txn.unit_price,
        txn.discount_percent,

        txn.line_total_clean AS line_total,
        txn.discount_amount,
        txn.subtotal,
        txn.tax_amount,
        txn.total_amount,

        txn.quantity_clean * txn.unit_price AS gross_amount,
        txn.line_total_clean - txn.discount_amount AS net_amount,

        txn.quantity_clean * COALESCE(prod.cost_price, 0) AS total_cost,
        txn.line_total_clean - (txn.quantity_clean * COALESCE(prod.cost_price, 0)) AS line_profit,

        CASE 
            WHEN txn.line_total_clean > 0 AND prod.cost_price IS NOT NULL AND prod.cost_price > 0
            THEN ((txn.line_total_clean - (txn.quantity_clean * prod.cost_price)) / txn.line_total_clean) * 100
            ELSE NULL
        END AS line_profit_margin_pct,

        txn.loyalty_points_earned,

        txn.data_quality_status,

        txn.had_negative_quantity,
        txn.had_negative_amount,
        txn.had_zero_total,
        txn.had_missing_time,
        txn.had_missing_cashier,
        txn.had_high_unit_price,

        txn.bronze_loaded_at,
        txn.staging_loaded_at AS fact_loaded_at,
        txn._source_system,
        CURRENT_TIMESTAMP AS created_at
    
    FROM sales_transactions txn

    LEFT JOIN {{ ref('dim_customers') }} cust
        ON txn.customer_id = cust.customer_id
        AND cust.is_current = TRUE
        
    LEFT JOIN {{ ref('dim_products') }} prod
        ON txn.product_id = prod.product_id
        AND prod.is_current = TRUE
        
    LEFT JOIN {{ ref('dim_stores') }} store
        ON txn.store_id = store.store_id
        AND store.is_current = TRUE
)

SELECT * FROM sales_with_dimension