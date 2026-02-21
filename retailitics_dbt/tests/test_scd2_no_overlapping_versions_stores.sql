{{
    config(
        severity='error',
        tags=['gold', 'scd2', 'data-integrity']
    )
}}

WITH store_versions AS (
    SELECT 
        store_id,
        store_key,
        valid_start_date,
        COALESCE(valid_end_date::TIMESTAMP, '2099-12-31'::TIMESTAMP) AS valid_end_date_adjusted, 
        is_current
    FROM {{ ref('dim_stores') }}
),

overlapping_versions AS (
    SELECT 
        v1.store_id,
        v1.store_key AS version_1_key,
        v2.store_key AS version_2_key,
        v1.valid_start_date AS v1_start,
        v1.valid_end_date_adjusted AS v1_end,
        v2.valid_start_date AS v2_start,
        v2.valid_end_date_adjusted AS v2_end
    FROM store_versions v1
    INNER JOIN store_versions v2
        ON v1.store_id = v2.store_id
        AND v1.store_key != v2.store_key
    WHERE 
        v1.valid_start_date < v2.valid_end_date_adjusted
        AND v2.valid_start_date < v1.valid_end_date_adjusted
)

SELECT 
    store_id,
    version_1_key,
    version_2_key,
    v1_start,
    v1_end,
    v2_start,
    v2_end,
    'OVERLAPPING VERSIONS!' AS error_message
FROM overlapping_versions