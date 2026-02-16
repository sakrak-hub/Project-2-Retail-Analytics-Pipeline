{{
    config(
        severity='warn',
        tags=['staging', 'business-logic']
    )
}}

WITH calculated AS (
    SELECT 
        line_item_key,
        transaction_id,
        product_id,
        line_total_clean,
        quantity_clean * unit_price * (1 - discount_percent/100.0) AS calculated_total,
        ABS(line_total_clean - (quantity_clean * unit_price * (1 - discount_percent/100.0))) AS difference
    FROM {{ ref('stg_transactions') }}
    WHERE 
        data_quality_status = 'CLEAN'  -- Only check clean records
        AND is_refund = FALSE  -- Exclude refunds
        AND quantity_clean > 0
        AND unit_price > 0
        AND line_total_clean <> 0
)

SELECT 
    line_item_key,
    transaction_id,
    product_id,
    line_total_clean,
    calculated_total,
    difference
FROM calculated
WHERE difference > 0.02