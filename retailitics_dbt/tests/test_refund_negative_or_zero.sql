{{
    config(
        severity='warn',
        tags=['staging', 'business-logic', 'refunds']
    )
}}

SELECT 
    line_item_key,
    transaction_id,
    product_id,
    is_refund,
    line_total_clean,
    quantity_clean
FROM {{ ref('stg_transactions') }}
WHERE 
    is_refund = TRUE
    AND line_total_clean > 0 