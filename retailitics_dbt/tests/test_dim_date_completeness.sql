{{
    config(
        severity='error',
        tags=['gold', 'dimension', 'data-integrity']
    )
}}

WITH expected_dates AS (
    SELECT 
        '2020-01-01'::DATE + INTERVAL (n) DAY AS expected_date
    FROM generate_series(0, 4017) AS gs(n)
),

actual_dates AS (
    SELECT 
        date_actual
    FROM {{ ref('dim_date') }}
),

missing_dates AS (
    SELECT 
        ed.expected_date 
    FROM expected_dates ed
    LEFT JOIN actual_dates ad
        ON ed.expected_date = ad.date_actual
    WHERE ad.date_actual IS NULL
)

SELECT 
    expected_date,
    'MISSING DATE!' AS error_message
FROM missing_dates