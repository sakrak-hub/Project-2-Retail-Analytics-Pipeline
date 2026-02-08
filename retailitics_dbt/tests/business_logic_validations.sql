SELECT 
    transaction_id,
    status,
    refund_reason
FROM {{ ref('bronze_transactions') }}
WHERE status = 'Refunded'
    AND (refund_reason IS NULL OR refund_reason = '')