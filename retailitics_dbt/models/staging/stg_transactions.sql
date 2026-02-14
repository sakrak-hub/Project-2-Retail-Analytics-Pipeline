{{
    config(
        materialized='incremental',
        unique_key=['transaction_id_clean', 'product_id'],
        on_schema_change='fail',
        tags=['staging','incremental']
    )
}}

WITH bronze AS (
    SELECT * FROM {{ ref('bronze_transactions') }}
    
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(
          PARTITION BY transaction_product
        ) AS row_num
    FROM bronze
    ORDER BY 1
),

staging_validated AS (
    SELECT
        transaction_id_clean,
        product_id,
        {{ dbt_utils.generate_surrogate_key(['transaction_id_clean', 'product_id']) }} AS line_item_key,
        customer_id,
        store_id,
        cashier_id,

        transaction_date,
        transaction_time,
        transaction_datetime,

        transaction_year,
        transaction_month,
        transaction_day,
        transaction_day_of_week,
        transaction_hour,

        quantity,
        unit_price,

        discount_percent,
        discount_amount,
        line_total,

        subtotal,
        tax_amount,
        total_amount,

        payment_method,
        promotion_code,
        payment_status,
        refund_reason,

        is_refund,
        has_promotion,

        CASE 
            WHEN ABS((quantity * unit_price) - line_total) > 0.01 THEN 1
            ELSE 0
        END AS line_total_calculation_error_flag,

        CASE 
            WHEN discount_amount > line_total THEN 1
            ELSE 0
        END AS excessive_discount_flag,

        CASE 
            WHEN ABS((subtotal + tax_amount) - total_amount) > 0.01 THEN 1
            ELSE 0
        END AS total_calculation_error_flag,

        CASE 
            WHEN subtotal > 0 AND (tax_amount / subtotal) > 0.15 THEN 1
            ELSE 0
        END AS unusual_tax_rate_flag,

        CASE 
            WHEN quantity IS NULL THEN (line_total / quantity)
            ELSE unit_price
        END AS effective_unit_price,

        CASE 
            WHEN subtotal > 0 THEN ROUND((tax_amount / subtotal) * 100, 2)
            ELSE 0
        END AS tax_rate_percentage,

        CASE 
            WHEN line_total >= 1000 THEN 'Large'
            WHEN line_total >= 100 THEN 'Medium'
            WHEN line_total >= 10 THEN 'Small'
            ELSE 'Micro'
        END AS line_item_size_category,

        CASE 
            WHEN total_amount >= 1000 THEN 'Large'
            WHEN total_amount >= 100 THEN 'Medium'
            WHEN total_amount >= 10 THEN 'Small'
            ELSE 'Micro'
        END AS transaction_size_category,

        missing_transaction_id_flag,
        missing_date_flag,
        missing_time_flag,
        missing_customer_id_flag,
        missing_product_id_flag,
        missing_cashier_id_flag,
        missing_payment_method_flag,
        negative_quantity_flag,
        negative_amount_flag,
        negative_line_total_flag,
        zero_total_amount_flag,
        zero_quantity_flag,
        high_unit_price_flag,
        high_total_amount_flag,

        (
            missing_transaction_id_flag +
            missing_date_flag +
            missing_time_flag +
            missing_customer_id_flag +
            missing_product_id_flag +
            missing_cashier_id_flag +
            missing_payment_method_flag +
            negative_quantity_flag +
            negative_amount_flag +
            negative_line_total_flag +
            zero_total_amount_flag +
            zero_quantity_flag +
            high_unit_price_flag +
            high_total_amount_flag +
            CASE WHEN ABS((quantity * unit_price) - line_total) > 0.01 THEN 1 ELSE 0 END +
            CASE WHEN ABS((subtotal + tax_amount) - total_amount) > 0.01 THEN 1 ELSE 0 END +
            CASE WHEN discount_amount > line_total THEN 1 ELSE 0 END +
            CASE WHEN subtotal > 0 AND (tax_amount / subtotal) > 0.15 THEN 1 ELSE 0 END
        ) AS quality_issues_count,

        CASE 
            WHEN (
                missing_transaction_id_flag +
                missing_date_flag +
                missing_time_flag +
                missing_customer_id_flag +
                missing_product_id_flag +
                missing_cashier_id_flag +
                missing_payment_method_flag +
                negative_quantity_flag +
                negative_amount_flag +
                negative_line_total_flag +
                zero_total_amount_flag +
                zero_quantity_flag +
                high_unit_price_flag +
                high_total_amount_flag +
                CASE WHEN ABS((quantity * unit_price) - line_total) > 0.01 THEN 1 ELSE 0 END +
                CASE WHEN ABS((subtotal + tax_amount) - total_amount) > 0.01 THEN 1 ELSE 0 END +
                CASE WHEN discount_amount > line_total THEN 1 ELSE 0 END +
                CASE WHEN subtotal > 0 AND (tax_amount / subtotal) > 0.15 THEN 1 ELSE 0 END
            ) = 0 THEN 'CLEAN'
            WHEN (
                missing_transaction_id_flag +
                missing_date_flag +
                missing_time_flag +
                missing_customer_id_flag +
                missing_product_id_flag +
                missing_cashier_id_flag +
                missing_payment_method_flag +
                negative_quantity_flag +
                negative_amount_flag +
                negative_line_total_flag +
                zero_total_amount_flag +
                zero_quantity_flag +
                high_unit_price_flag +
                high_total_amount_flag +
                CASE WHEN ABS((quantity * unit_price) - line_total) > 0.01 THEN 1 ELSE 0 END +
                CASE WHEN ABS((subtotal + tax_amount) - total_amount) > 0.01 THEN 1 ELSE 0 END +
                CASE WHEN discount_amount > line_total THEN 1 ELSE 0 END +
                CASE WHEN subtotal > 0 AND (tax_amount / subtotal) > 0.15 THEN 1 ELSE 0 END
            ) <= 2 THEN 'ACCEPTABLE'
            ELSE 'POOR'
        END AS quality_tier,

        _loaded_at,
        _batch_id,
        _source_system,
        CURRENT_TIMESTAMP AS _staging_loaded_at
        
    FROM deduplicated

    WHERE missing_transaction_id_flag = 0      
      AND missing_date_flag = 0               
      AND missing_customer_id_flag = 0        
      AND missing_product_id_flag = 0
      AND (
          missing_transaction_id_flag +
          missing_date_flag +
          missing_time_flag +
          missing_customer_id_flag +
          missing_product_id_flag +
          missing_cashier_id_flag +
          missing_payment_method_flag +
          negative_quantity_flag +
          negative_amount_flag +
          negative_line_total_flag +
          zero_total_amount_flag +
          zero_quantity_flag
      ) <= 3  

)

SELECT * FROM staging_validated