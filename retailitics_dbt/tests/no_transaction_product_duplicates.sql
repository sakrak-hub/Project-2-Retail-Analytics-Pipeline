{{
    config(
        severity='error',
        tags=['staging', 'deduplication', 'critical']
    )
}}

SELECT 
    transaction_id,
    product_id,
    COUNT(*) as duplicate_count
FROM {{ ref('stg_transactions') }}
GROUP BY 
    transaction_id, 
    product_id
HAVING COUNT(*) > 1