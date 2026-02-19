{{
    config(
        materialized='incremental',
        unique_key='line_item_key',
        on_schema_change='fail',
        tags=['staging', 'silver', 'transactions']
    )
}}

WITH max_loaded AS (
    SELECT COALESCE(MAX(_loaded_at), '2026-01-01'::TIMESTAMP) AS max_load
        FROM {{ ref('bronze_transactions') }}
),

bronze_source AS (
    SELECT * 
    FROM {{ ref('bronze_transactions') }}
    
    {% if is_incremental() %}
    WHERE _loaded_at > (
        SELECT max_load
        FROM max_loaded
    )
    {% endif %}
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id_clean, product_id 
            ORDER BY 
                _loaded_at DESC,
                CASE WHEN is_refund THEN 0 ELSE 1 END DESC
        ) AS row_num
    FROM bronze_source
),

filtered AS (
    SELECT *
    FROM deduplicated
    WHERE row_num = 1
    
    AND missing_transaction_id_flag = 0
    AND missing_date_flag = 0
    AND missing_customer_id_flag = 0
    AND missing_product_id_flag = 0
    
    AND NOT (zero_total_amount_flag = 1 AND is_refund = FALSE)
),

staging_final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'transaction_id_clean', 
            'product_id', 
            '_loaded_at'
        ]) }} AS line_item_key,
        
        transaction_id_clean AS transaction_id,
        product_id,
        customer_id,
        store_id,
        store_name,
        cashier_id,

        transaction_date,
        transaction_time,
        transaction_datetime,
        transaction_year,
        transaction_month,
        transaction_day,
        transaction_day_of_week,
        transaction_hour,

        CASE 
            WHEN transaction_day_of_week IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend,
        
        CASE 
            WHEN transaction_day_of_week = 0 THEN 'Sunday'
            WHEN transaction_day_of_week = 1 THEN 'Monday'
            WHEN transaction_day_of_week = 2 THEN 'Tuesday'
            WHEN transaction_day_of_week = 3 THEN 'Wednesday'
            WHEN transaction_day_of_week = 4 THEN 'Thursday'
            WHEN transaction_day_of_week = 5 THEN 'Friday'
            WHEN transaction_day_of_week = 6 THEN 'Saturday'
        END AS day_name,
        
        CASE 
            WHEN transaction_hour IS NULL THEN 'Unknown'
            WHEN transaction_hour BETWEEN 6 AND 11 THEN 'Morning (6-11am)'
            WHEN transaction_hour BETWEEN 12 AND 16 THEN 'Afternoon (12-4pm)'
            WHEN transaction_hour BETWEEN 17 AND 20 THEN 'Evening (5-8pm)'
            WHEN transaction_hour BETWEEN 21 AND 23 THEN 'Night (9-11pm)'
            ELSE 'Late Night (12-5am)'
        END AS time_of_day,

        payment_method,
        payment_status,

        subtotal,
        tax_amount,
        total_amount,
        items_count,
        loyalty_points_earned,

        product_name,
        category,
        quantity,
        unit_price,
        discount_percent,
        discount_amount,
        line_total,

        CASE 
            WHEN line_total < 0 AND is_refund = FALSE THEN 0
            WHEN line_total = 0 AND quantity > 0 AND unit_price > 0 
                THEN quantity * unit_price * (1 - discount_percent/100)
            ELSE line_total
        END AS line_total_clean,
        
        CASE 
            WHEN quantity < 0 AND is_refund = FALSE THEN ABS(quantity)
            ELSE quantity
        END AS quantity_clean,

        is_refund,
        refund_reason,
        has_promotion,
        promotion_code,

        CASE 
            WHEN discount_percent > 0 THEN TRUE
            ELSE FALSE
        END AS has_discount,
        
        CASE 
            WHEN quantity > 1 THEN TRUE
            ELSE FALSE
        END AS is_multi_quantity,
        
        CASE 
            WHEN quantity >= 10 THEN TRUE
            ELSE FALSE
        END AS is_bulk_purchase,
        
        CASE 
            WHEN line_total > 1000 THEN TRUE
            ELSE FALSE
        END AS is_high_value_item,
        
        CASE 
            WHEN loyalty_points_earned > 0 THEN TRUE
            ELSE FALSE
        END AS earned_loyalty_points,

        CASE 
            WHEN is_refund = TRUE THEN 'Refund'
            WHEN has_promotion = TRUE AND discount_percent > 25 THEN 'Promotional Sale'
            WHEN has_promotion = TRUE THEN 'Promotion'
            WHEN discount_percent > 25 THEN 'Deep Discount'
            WHEN discount_percent > 0 THEN 'Discounted'
            ELSE 'Regular Sale'
        END AS transaction_type,

        CASE 
            WHEN (negative_quantity_flag = 1 
                OR negative_amount_flag = 1 
                OR negative_line_total_flag = 1
                OR zero_total_amount_flag = 1)
                AND is_refund = FALSE
            THEN 'CORRECTED'
            WHEN missing_time_flag = 1
                OR missing_cashier_id_flag = 1
            THEN 'INCOMPLETE'
            ELSE 'CLEAN'
        END AS data_quality_status,

        negative_quantity_flag AS had_negative_quantity,
        negative_amount_flag AS had_negative_amount,
        zero_total_amount_flag AS had_zero_total,
        missing_time_flag AS had_missing_time,
        missing_cashier_id_flag AS had_missing_cashier,
        high_unit_price_flag AS had_high_unit_price,

        _loaded_at AS bronze_loaded_at,
        CURRENT_TIMESTAMP AS staging_loaded_at,
        _batch_id AS bronze_batch_id,
        '{{ invocation_id }}' AS staging_batch_id,
        _source_system
        
    FROM filtered
)

SELECT * FROM staging_final