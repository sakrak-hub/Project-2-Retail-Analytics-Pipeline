{{
    config(
        severity='error',
        tags=['gold', 'scd2', 'data-integrity']
    )
}}

WITH product_versions AS (
    SELECT 
        product_id,
        product_key,
        valid_start_date,
        COALESCE(valid_end_date::TIMESTAMP, '2099-12-31'::TIMESTAMP) AS valid_end_date_adjusted,
        is_current
    FROM {{ ref('dim_products') }}
),

overlapping_versions AS (
    SELECT 
        v1.product_id,
        v1.product_key AS version_1_key,
        v2.product_key AS version_2_key,
        v1.valid_start_date AS v1_start,
        v1.valid_end_date_adjusted AS v1_end,
        v2.valid_start_date AS v2_start,
        v2.valid_end_date_adjusted AS v2_end
    FROM product_versions v1
    INNER JOIN product_versions v2
        ON v1.product_id = v2.product_id
        AND v1.product_key != v2.product_key
    WHERE 
        v1.valid_start_date < v2.valid_end_date_adjusted
        AND v2.valid_start_date < v1.valid_end_date_adjusted
)

SELECT 
    product_id,
    version_1_key,
    version_2_key,
    v1_start,
    v1_end,
    v2_start,
    v2_end,
    'OVERLAPPING VERSIONS!' AS error_message
FROM overlapping_versions