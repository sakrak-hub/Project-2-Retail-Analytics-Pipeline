SELECT 
    transaction_id_clean, 
    payment_status,
    refund_reason
FROM {{ ref('bronze_transactions') }}
WHERE payment_status = 'Refunded'
    AND (refund_reason IS NULL OR refund_reason = '')