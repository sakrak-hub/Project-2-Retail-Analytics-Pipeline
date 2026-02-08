WITH transaction_validation AS (
    SELECT 
        transaction_id,
        subtotal,
        tax_amount,
        total_amount,
        ABS((subtotal + tax_amount) - total_amount) as difference
    FROM {{ ref('bronze_transactions') }}
)
SELECT *
FROM transaction_validation
WHERE difference > 0.01 