{{
    config(
        severity='warn',
        tags=['gold', 'fact', 'business-logic']
    )
}}

WITH profit_check AS (
    SELECT 
        sales_key,
        transaction_id,
        product_key,
        line_total,
        total_cost,
        line_profit,
        line_total - total_cost AS calculated_profit,
        ABS(line_profit - (line_total - total_cost)) AS difference
    FROM {{ ref('fact_sales') }}
    WHERE 
        total_cost IS NOT NULL
        AND total_cost > 0
        AND line_total <> 0
        AND is_refund = FALSE
)

SELECT 
    sales_key,
    transaction_id,
    product_key,
    line_total,
    total_cost,
    line_profit,
    calculated_profit,
    difference,
    'Profit calculation mismatch!' AS error_message
FROM profit_check
WHERE 
    difference > 0.02