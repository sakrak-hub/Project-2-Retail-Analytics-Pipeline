{{
    config(
        severity='warn',
        tags=['gold', 'fact', 'business-logic']
    )
}}

SELECT 
    sales_key,
    transaction_id,
    product_key,
    customer_key,
    is_refund,
    quantity,
    unit_price,
    line_total,
    CASE 
        WHEN line_total <= 0 THEN 'Line total should be positive'
        WHEN quantity <= 0 THEN 'Quantity should be positive'
        WHEN unit_price <= 0 THEN 'Unit price should be positive'
    END AS error_reason
FROM {{ ref('fact_sales') }}
WHERE 
    is_refund = FALSE
    AND (
        line_total <= 0
        OR quantity <= 0
        OR unit_price <= 0
    )