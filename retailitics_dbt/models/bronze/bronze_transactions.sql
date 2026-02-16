{{
    config(
        materialized='incremental',
        unique_key=['transaction_id_clean', 'product_id'],
        on_schema_change='fail',
        tags=['bronze', 'incremental']
    )
}}

WITH source_data AS (
    SELECT * FROM {{ ref('raw_transactions') }}
    
    {% if is_incremental() %}

    WHERE NOT EXISTS (
        SELECT 1 
        FROM {{ this }} existing
        WHERE existing.transaction_id_clean = REGEXP_REPLACE({{ ref('raw_transactions') }}.transaction_id, 'DUP', '', 'g')
          AND existing.product_id = {{ ref('raw_transactions') }}.product_id
          AND existing._row_hash = MD5(
              COALESCE({{ ref('raw_transactions') }}.transaction_id, '') || '|' ||
              COALESCE({{ ref('raw_transactions') }}.product_id, '') || '|' ||
              COALESCE(CAST({{ ref('raw_transactions') }}.quantity AS VARCHAR), '') || '|' ||
              COALESCE(CAST({{ ref('raw_transactions') }}.unit_price AS VARCHAR), '') || '|' ||
              COALESCE(CAST({{ ref('raw_transactions') }}.line_total AS VARCHAR), '') || '|' ||
              COALESCE({{ ref('raw_transactions') }}.status, '')
          )
    )
    {% endif %}
),

bronze_cleaned AS (
    SELECT
        REGEXP_REPLACE(transaction_id, 'DUP', '', 'g') AS transaction_id_clean,
        transaction_id AS transaction_id_raw,
        REGEXP_REPLACE(transaction_id || '-' || product_id, 'DUP', '', 'g') AS transaction_product,

        CASE 
            WHEN date IS NULL OR TRIM(date::VARCHAR) = '' THEN strptime(regexp_extract(transaction_id, 'TXN(\d{8})', 1), '%Y%m%d')::DATE
            ELSE TRY_CAST(date AS DATE)
        END AS transaction_date,

        CASE 
            WHEN time IS NULL OR TRIM(time::VARCHAR) = '' THEN '00:00:00'::TIME
            ELSE TRY_CAST(time AS TIME)
        END AS transaction_time,

        CASE 
            WHEN date IS NULL OR TRIM(date::VARCHAR) = '' OR time IS NULL OR TRIM(time::VARCHAR) = '' 
            THEN strptime(regexp_extract(transaction_id, 'TXN(\d{8})', 1), '%Y%m%d')::DATE + '00:00:00'::TIME
            ELSE TRY_CAST(date AS TIMESTAMP) + TRY_CAST(time AS INTERVAL)
        END AS transaction_datetime,

        CASE 
            WHEN date IS NULL OR TRIM(date::VARCHAR) = '' THEN YEAR(strptime(regexp_extract(transaction_id, 'TXN(\d{8})', 1), '%Y%m%d')::DATE)
            ELSE YEAR(TRY_CAST(date AS DATE))
        END AS transaction_year,
        
        CASE 
            WHEN date IS NULL OR TRIM(date::VARCHAR) = '' THEN MONTH(strptime(regexp_extract(transaction_id, 'TXN(\d{8})', 1), '%Y%m%d')::DATE)
            ELSE MONTH(TRY_CAST(date AS DATE))
        END AS transaction_month,
        
        CASE 
            WHEN date IS NULL OR TRIM(date::VARCHAR) = '' THEN DAY(strptime(regexp_extract(transaction_id, 'TXN(\d{8})', 1), '%Y%m%d')::DATE)
            ELSE DAY(TRY_CAST(date AS DATE))
        END AS transaction_day,

        CASE 
            WHEN date IS NULL OR TRIM(date::VARCHAR) = '' THEN DAYOFWEEK(strptime(regexp_extract(transaction_id, 'TXN(\d{8})', 1), '%Y%m%d')::DATE)
            ELSE DAYOFWEEK(TRY_CAST(date AS DATE))
        END AS transaction_day_of_week,

        CASE 
            WHEN time IS NULL OR TRIM(time::VARCHAR) = '' THEN NULL
            ELSE HOUR(TRY_CAST(time AS TIME))
        END AS transaction_hour,

        customer_id,
        store_id,
        TRIM(REGEXP_REPLACE(store_name, '[^\x20-\x7E]', '', 'g')) AS store_name,
        cashier_id,

        UPPER(TRIM(payment_method)) AS payment_method,
        UPPER(TRIM(status)) AS payment_status,

        CASE 
            WHEN status = 'Refunded' OR refund_reason IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_refund,

        TRIM(refund_reason) AS refund_reason,
        UPPER(TRIM(promotion_code)) AS promotion_code,

        CASE 
            WHEN promotion_code IS NOT NULL AND TRIM(promotion_code) != '' THEN TRUE
            ELSE FALSE
        END AS has_promotion,

        subtotal,
        tax_amount,
        total_amount, 
        items_count,
        loyalty_points_earned,

        product_id,
        TRIM(REGEXP_REPLACE(product_name, '[^\x20-\x7E]', '', 'g')) AS product_name,
        TRIM(category) AS category,
        quantity,
        ROUND(unit_price,2) AS unit_price,
        discount_percent,
        (quantity * unit_price * (discount_percent/100)) AS discount_amount,
        line_total,

        CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END AS missing_transaction_id_flag,
        CASE WHEN date IS NULL OR TRIM(date::VARCHAR) = '' THEN 1 ELSE 0 END AS missing_date_flag,
        CASE WHEN time IS NULL OR TRIM(time::VARCHAR) = '' THEN 1 ELSE 0 END AS missing_time_flag,
        CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END AS missing_customer_id_flag,
        CASE WHEN product_id IS NULL THEN 1 ELSE 0 END AS missing_product_id_flag,
        CASE WHEN cashier_id IS NULL OR TRIM(cashier_id) = '' THEN 1 ELSE 0 END AS missing_cashier_id_flag,
        CASE WHEN payment_method IS NULL OR TRIM(payment_method) = '' THEN 1 ELSE 0 END AS missing_payment_method_flag,

        CASE WHEN quantity < 0 THEN 1 ELSE 0 END AS negative_quantity_flag,
        CASE WHEN total_amount < 0 THEN 1 ELSE 0 END AS negative_amount_flag,
        CASE WHEN line_total < 0 THEN 1 ELSE 0 END AS negative_line_total_flag,

        CASE WHEN total_amount IS NULL OR total_amount = 0 THEN 1 ELSE 0 END AS zero_total_amount_flag,
        CASE WHEN quantity IS NULL OR quantity = 0 THEN 1 ELSE 0 END AS zero_quantity_flag,

        CASE WHEN unit_price > 10000 THEN 1 ELSE 0 END AS high_unit_price_flag,
        CASE WHEN total_amount > 100000 THEN 1 ELSE 0 END AS high_total_amount_flag,

        CURRENT_TIMESTAMP AS _loaded_at, 
        '{{ run_started_at }}' AS _batch_id,
        '{{ var("source_system", "RETAIL_S3") }}' AS _source_system,

        MD5(
            COALESCE(transaction_id, '') || '|' ||
            COALESCE(product_id, '') || '|' ||
            COALESCE(CAST(quantity AS VARCHAR), '') || '|' ||
            COALESCE(CAST(unit_price AS VARCHAR), '') || '|' ||
            COALESCE(CAST(line_total AS VARCHAR), '') || '|' ||
            COALESCE(CAST(total_amount AS VARCHAR), '') || '|' ||
            COALESCE(status, '')
        ) AS _row_hash,

        'raw_transactions' AS _source_table
    
    FROM source_data
)

SELECT * FROM bronze_cleaned