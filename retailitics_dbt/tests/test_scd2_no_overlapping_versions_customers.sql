{{
    config(
        severity='error',
        tags=['gold', 'scd2', 'data-integrity']
    )
}}

WITH customer_versions AS (
    SELECT 
        customer_id,
        customer_key,
        valid_start_date,
        COALESCE(valid_end_date::TIMESTAMP, '2099-12-31'::TIMESTAMP) AS valid_end_date_adjusted,
        is_current
    FROM {{ ref('dim_customers') }}
),

overlapping_versions AS (
    SELECT 
        v1.customer_id,
        v1.customer_key AS version_1_key,
        v2.customer_key AS version_2_key,
        v1.valid_start_date AS v1_start,
        v1.valid_end_date_adjusted AS v1_end,
        v2.valid_start_date AS v2_start,
        v2.valid_end_date_adjusted AS v2_end
    FROM customer_versions v1
    INNER JOIN customer_versions v2
        ON v1.customer_id = v2.customer_id
        AND v1.customer_key != v2.customer_key
    WHERE 
        v1.valid_start_date < v2.valid_end_date_adjusted
        AND v2.valid_start_date < v1.valid_end_date_adjusted
)

SELECT 
    customer_id,
    version_1_key,
    version_2_key,
    v1_start,
    v1_end,
    v2_start,
    v2_end,
    'OVERLAPPING VERSIONS!' AS error_message
FROM overlapping_versions