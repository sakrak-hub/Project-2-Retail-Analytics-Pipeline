{{
    config(
        materialized='table',
        tags=['mart', 'customer']
    )
}}

WITH customer_purchases AS (
    SELECT 
        customer_key,
        
        COUNT(DISTINCT transaction_id) as total_purchases,
        COUNT(DISTINCT transaction_date::DATE) as days_purchased,
        COUNT(DISTINCT DATE_TRUNC('month', transaction_date)) as months_active,
        
        
        SUM(line_total) as lifetime_revenue,
        AVG(line_total) as avg_purchase_value,
        
        
        MIN(transaction_date::DATE) as first_purchase_date,
        MAX(transaction_date::DATE) as last_purchase_date,
        DATEDIFF('day', MIN(transaction_date::DATE), MAX(transaction_date::DATE)) as customer_lifespan_days,
        
        
        COUNT(DISTINCT transaction_id) / NULLIF(
            DATEDIFF('month', MIN(transaction_date), MAX(transaction_date)) + 1, 0
        ) as purchases_per_month
        
    FROM {{ ref('fact_sales') }}
    WHERE is_refund = FALSE
    GROUP BY customer_key
),

customer_details AS (
    SELECT 
        c.customer_id,
        c.full_name,
        c.customer_segment,
        c.age_group,
        c.city,
        c.state,
        c.registration_date,
        cp.*
        
    FROM customer_purchases cp
    INNER JOIN {{ ref('dim_customers') }} c 
        ON cp.customer_key = c.customer_key
        AND c.is_current = TRUE
)

SELECT 
    
    customer_id,
    full_name,
    customer_segment,
    age_group,
    city,
    state,
    
    total_purchases,
    days_purchased,
    months_active,
    
    ROUND(lifetime_revenue, 2) as lifetime_revenue,
    ROUND(avg_purchase_value, 2) as avg_purchase_value,
    
    first_purchase_date,
    last_purchase_date,
    registration_date,
    customer_lifespan_days,
    DATEDIFF('day', last_purchase_date, CURRENT_DATE) as days_since_last_purchase,
    
    ROUND(purchases_per_month, 1) as purchases_per_month,
    
    CASE 
        WHEN total_purchases = 1 THEN 'One-Time Buyer'
        WHEN total_purchases = 2 THEN 'Occasional Buyer (2 purchases)'
        WHEN total_purchases <= 5 THEN 'Regular Buyer (3-5 purchases)'
        WHEN total_purchases <= 10 THEN 'Frequent Buyer (6-10 purchases)'
        ELSE 'VIP Buyer (10+ purchases)'
    END as buyer_type,
    
    CASE 
        WHEN DATEDIFF('day', last_purchase_date, CURRENT_DATE) <= 30 THEN 'Active (Last 30 days)'
        WHEN DATEDIFF('day', last_purchase_date, CURRENT_DATE) <= 60 THEN 'Recent (Last 60 days)'
        WHEN DATEDIFF('day', last_purchase_date, CURRENT_DATE) <= 90 THEN 'At Risk (60-90 days)'
        ELSE 'Inactive (90+ days)'
    END as activity_status,
    
    CASE 
        WHEN lifetime_revenue >= 1000 THEN 'High Value ($1000+)'
        WHEN lifetime_revenue >= 500 THEN 'Medium Value ($500-$1000)'
        WHEN lifetime_revenue >= 100 THEN 'Low Value ($100-$500)'
        ELSE 'Very Low Value (<$100)'
    END as value_tier,
    
    CASE 
        WHEN total_purchases = 1 THEN 'Single Purchase'
        WHEN customer_lifespan_days = 0 THEN 'Same Day Purchases'
        WHEN customer_lifespan_days / NULLIF(total_purchases - 1, 0) <= 7 THEN 'Weekly Buyer'
        WHEN customer_lifespan_days / NULLIF(total_purchases - 1, 0) <= 30 THEN 'Monthly Buyer'
        ELSE 'Irregular Buyer'
    END as purchase_pattern
    
FROM customer_details
ORDER BY lifetime_revenue DESC