WITH transaction_validation AS (
    SELECT 
        transaction_id_clean,
        subtotal,
        tax_amount,
        total_amount,
        ROUND(ABS((subtotal + tax_amount) - total_amount),2) as difference
    FROM {{ ref('bronze_transactions') }}
)
SELECT *
FROM transaction_validation
WHERE difference > 0.01 