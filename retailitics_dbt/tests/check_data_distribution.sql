WITH payment_distribution AS (
    SELECT 
        payment_method,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
    FROM {{ ref('bronze_transactions')}}
    GROUP BY payment_method
)
SELECT *
FROM payment_distribution
WHERE percentage < 1.0
    OR percentage > 80.0