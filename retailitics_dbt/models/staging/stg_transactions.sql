{{
    config(
        materialized='incremental',
        unique_key=['transaction_id_clean', 'product_id'],
        on_schema_change='fail',
        tags=['staging', 'incremental']
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

store_name_clean AS(
    WITH stores AS(
        SELECT DISTINCT(store_name) AS store_name_raw,
        store_id
        FROM {{ ref('raw_transactions')}}
    )

    SELECT
    store_id,
    TRIM(REGEXP_REPLACE(store_name_raw, '[^\x20-\x7E]', '', 'g')) AS store_name_clean,
    CASE 
    WHEN store_name_raw = TRIM(REGEXP_REPLACE(store_name_raw, '[^\x20-\x7E]', '', 'g')) THEN 'Clean'
    ELSE 'Processed'
    END AS processed_flag
    FROM stores
    WHERE processed_flag='Clean'
    ORDER BY 1
),

staging_cleaned AS (
    SELECT
        REGEXP_REPLACE(sd.transaction_id, 'DUP', '', 'g') AS transaction_id_clean,
        sd.transaction_id AS transaction_id_raw,
        REGEXP_REPLACE(sd.transaction_id || '-' || product_id, 'DUP', '', 'g') AS transaction_product,

        CASE 
            WHEN sd.date IS NULL OR TRIM(sd.date::VARCHAR) = '' THEN strptime(regexp_extract(sd.transaction_id, 'TXN(\d{8})', 1), '%Y%m%d')::DATE
            ELSE TRY_CAST(sd.date AS DATE)
        END AS transaction_date,

        CASE 
            WHEN sd.time IS NULL OR TRIM(sd.time::VARCHAR) = '' THEN '00:00:00'::TIME
            ELSE TRY_CAST(sd.time AS TIME)
        END AS transaction_time,

        CASE 
            WHEN sd.date IS NULL OR TRIM(sd.date::VARCHAR) = '' OR sd.time IS NULL OR TRIM(sd.time::VARCHAR) = '' 
            THEN strptime(regexp_extract(sd.transaction_id, 'TXN(\d{8})', 1), '%Y%m%d')::DATE + '00:00:00'::TIME
            ELSE TRY_CAST(sd.date AS TIMESTAMP) + TRY_CAST(sd.time AS INTERVAL)
        END AS transaction_datetime,

        CASE 
            WHEN sd.date IS NULL OR TRIM(sd.date::VARCHAR) = '' THEN YEAR(strptime(regexp_extract(sd.transaction_id, 'TXN(\d{8})', 1), '%Y%m%d')::DATE)
            ELSE YEAR(TRY_CAST(sd.date AS DATE))
        END AS transaction_year,
        
        CASE 
            WHEN sd.date IS NULL OR TRIM(sd.date::VARCHAR) = '' THEN MONTH(strptime(regexp_extract(sd.transaction_id, 'TXN(\d{8})', 1), '%Y%m%d')::DATE)
            ELSE MONTH(TRY_CAST(sd.date AS DATE))
        END AS transaction_month,
        
        CASE 
            WHEN sd.date IS NULL OR TRIM(sd.date::VARCHAR) = '' THEN DAY(strptime(regexp_extract(sd.transaction_id, 'TXN(\d{8})', 1), '%Y%m%d')::DATE)
            ELSE DAY(TRY_CAST(sd.date AS DATE))
        END AS transaction_day,

        CASE 
            WHEN sd.date IS NULL OR TRIM(sd.date::VARCHAR) = '' THEN DAYOFWEEK(strptime(regexp_extract(sd.transaction_id, 'TXN(\d{8})', 1), '%Y%m%d')::DATE)
            ELSE DAYOFWEEK(TRY_CAST(sd.date AS DATE))
        END AS transaction_day_of_week,

        CASE 
            WHEN sd.time IS NULL OR TRIM(sd.time::VARCHAR) = '' THEN NULL
            ELSE HOUR(TRY_CAST(sd.time AS TIME))
        END AS transaction_hour,

        sd.customer_id,
        sd.store_id,
        sc.store_name_clean AS store_name,
        sd.cashier_id,

        UPPER(TRIM(sd.payment_method)) AS payment_method,
        UPPER(TRIM(sd.status)) AS payment_status,

        CASE 
            WHEN sd.status = 'Refunded' OR sd.refund_reason IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_refund,

        TRIM(sd.refund_reason) AS refund_reason,
        UPPER(TRIM(sd.promotion_code)) AS promotion_code,

        CASE 
            WHEN sd.promotion_code IS NOT NULL AND TRIM(sd.promotion_code) != '' THEN TRUE
            ELSE FALSE
        END AS has_promotion,

        sd.subtotal,
        sd.tax_amount,
        sd.total_amount, 
        sd.items_count,
        sd.loyalty_points_earned,

        sd.product_id,
        TRIM(REGEXP_REPLACE(sd.product_name, '[^\x20-\x7E]', '', 'g')) AS product_name,
        TRIM(sd.category) AS category,
        sd.quantity,
        ROUND(sd.unit_price,2) AS unit_price,
        sd.discount_percent,
        (sd.quantity * sd.unit_price * (sd.discount_percent/100)) AS discount_amount,
        sd.line_total,

        CASE WHEN sd.transaction_id IS NULL THEN 1 ELSE 0 END AS missing_transaction_id_flag,
        CASE WHEN sd.date IS NULL OR TRIM(date::VARCHAR) = '' THEN 1 ELSE 0 END AS missing_date_flag,
        CASE WHEN sd.time IS NULL OR TRIM(time::VARCHAR) = '' THEN 1 ELSE 0 END AS missing_time_flag,
        CASE WHEN sd.customer_id IS NULL THEN 1 ELSE 0 END AS missing_customer_id_flag,
        CASE WHEN sd.product_id IS NULL THEN 1 ELSE 0 END AS missing_product_id_flag,
        CASE WHEN sd.cashier_id IS NULL OR TRIM(cashier_id) = '' THEN 1 ELSE 0 END AS missing_cashier_id_flag,
        CASE WHEN sd.payment_method IS NULL OR TRIM(payment_method) = '' THEN 1 ELSE 0 END AS missing_payment_method_flag,

        CASE WHEN sd.quantity < 0 THEN 1 ELSE 0 END AS negative_quantity_flag,
        CASE WHEN sd.total_amount < 0 THEN 1 ELSE 0 END AS negative_amount_flag,
        CASE WHEN sd.line_total < 0 THEN 1 ELSE 0 END AS negative_line_total_flag,

        CASE WHEN sd.total_amount IS NULL OR total_amount = 0 THEN 1 ELSE 0 END AS zero_total_amount_flag,
        CASE WHEN sd.quantity IS NULL OR quantity = 0 THEN 1 ELSE 0 END AS zero_quantity_flag,

        CASE WHEN sd.unit_price > 10000 THEN 1 ELSE 0 END AS high_unit_price_flag,
        CASE WHEN sd.total_amount > 100000 THEN 1 ELSE 0 END AS high_total_amount_flag,

        sd._loaded_at_date AS raw_loaded_at,
        CURRENT_TIMESTAMP AS staging_processed_at, 
        '{{ run_started_at }}' AS staging_batch_id,
        '{{ var("source_system", "RETAIL_S3") }}' AS _source_system,

        MD5(
            COALESCE(sd.transaction_id, '') || '|' ||
            COALESCE(sd.product_id, '') || '|' ||
            COALESCE(CAST(sd.quantity AS VARCHAR), '') || '|' ||
            COALESCE(CAST(sd.unit_price AS VARCHAR), '') || '|' ||
            COALESCE(CAST(sd.line_total AS VARCHAR), '') || '|' ||
            COALESCE(CAST(sd.total_amount AS VARCHAR), '') || '|' ||
            COALESCE(status, '')
        ) AS _row_hash,

        'raw_transactions' AS _source_table
    
    FROM source_data sd
    LEFT JOIN store_name_clean sc 
    ON sd.store_id = sc.store_id
)

SELECT * FROM staging_cleaned