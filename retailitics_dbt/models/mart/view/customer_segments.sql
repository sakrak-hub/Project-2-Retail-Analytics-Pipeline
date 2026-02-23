{{
    config(
        materialized='table',
        tags=['mart', 'aggregated', 'customer', 'business']
    )
}}

WITH customer_sales AS (
    SELECT 
        
        c.customer_id,
        c.customer_segment,
        c.ltv_category,
        c.customer_tenure_category,
        c.age_group,
        c.gender,
        c.state,
        c.city,
        c.registration_date,
        c.days_since_registration,
        
        
        COUNT(DISTINCT f.sales_key) as total_orders,
        COUNT(DISTINCT f.transaction_id) as total_transactions,
        COUNT(DISTINCT DATE(f.transaction_date)) as active_days,
        SUM(f.line_total) as total_revenue,
        SUM(f.line_profit) as total_profit,
        SUM(f.quantity) as total_units_purchased,
        AVG(f.line_total) as avg_order_value,
        
        
        MIN(f.transaction_date) as first_purchase_date,
        MAX(f.transaction_date) as last_purchase_date,
        DATEDIFF('day', MIN(f.transaction_date), MAX(f.transaction_date)) as customer_lifespan_days,
        
        
        SUM(f.line_total) FILTER (WHERE f.has_promotion = TRUE) as promotional_revenue,
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.has_discount = TRUE) as discounted_orders,
        SUM(f.loyalty_points_earned) as total_loyalty_points,
        
        
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.is_refund = TRUE) as refund_count,
        SUM(f.line_total) FILTER (WHERE f.is_refund = TRUE) as refund_amount
        
    FROM {{ ref('fact_sales') }} f
    INNER JOIN {{ ref('dim_customers') }} c 
        ON f.customer_key = c.customer_key
        AND c.is_current = TRUE  
    
    GROUP BY 
        c.customer_id, c.customer_segment, c.ltv_category, c.customer_tenure_category,
        c.age_group, c.gender, c.state, c.city, c.registration_date, c.days_since_registration
),

segment_aggregates AS (
    SELECT 
        
        customer_segment,
        ltv_category,
        customer_tenure_category,
        age_group,
        gender,
        
        
        COUNT(DISTINCT customer_id) as customer_count,
        
        
        SUM(total_revenue) as segment_revenue,
        SUM(total_profit) as segment_profit,
        AVG(total_revenue) as avg_customer_revenue,
        AVG(total_profit) as avg_customer_profit,
        
        
        SUM(total_orders) as total_orders,
        AVG(total_orders) as avg_orders_per_customer,
        AVG(avg_order_value) as avg_order_value,
        
        
        AVG(active_days) as avg_active_days,
        AVG(customer_lifespan_days) as avg_customer_lifespan_days,
        AVG(total_orders * 1.0 / NULLIF(customer_lifespan_days, 0) * 30) as avg_orders_per_month,
        
        
        SUM(total_loyalty_points) as total_loyalty_points,
        AVG(total_loyalty_points) as avg_loyalty_points_per_customer,
        
        
        SUM(promotional_revenue) as total_promotional_revenue,
        AVG(promotional_revenue / NULLIF(total_revenue, 0) * 100) as avg_promotional_pct,
        AVG(discounted_orders * 100.0 / NULLIF(total_orders, 0)) as avg_discount_rate_pct,
        
        
        SUM(refund_count) as total_refunds,
        SUM(refund_amount) as total_refund_amount,
        AVG(refund_count * 100.0 / NULLIF(total_orders, 0)) as avg_refund_rate_pct,
        
        
        AVG(DATEDIFF('day', last_purchase_date, CURRENT_DATE)) as avg_days_since_last_purchase,
        COUNT(DISTINCT customer_id) FILTER (
            WHERE DATEDIFF('day', last_purchase_date, CURRENT_DATE) <= 30
        ) as active_last_30_days,
        COUNT(DISTINCT customer_id) FILTER (
            WHERE DATEDIFF('day', last_purchase_date, CURRENT_DATE) <= 90
        ) as active_last_90_days
        
    FROM customer_sales
    
    GROUP BY 
        customer_segment, ltv_category, customer_tenure_category, age_group, gender
),

final AS (
    SELECT 
        *,
        
        ROUND(segment_profit / NULLIF(segment_revenue, 0) * 100, 2) as profit_margin_pct,
        ROUND(avg_customer_revenue / NULLIF(avg_customer_lifespan_days, 0) * 365, 2) as annualized_revenue_per_customer,
        ROUND(total_promotional_revenue / NULLIF(segment_revenue, 0) * 100, 2) as promotional_mix_pct,
        ROUND(active_last_30_days * 100.0 / NULLIF(customer_count, 0), 2) as active_rate_30d_pct,
        ROUND(active_last_90_days * 100.0 / NULLIF(customer_count, 0), 2) as active_rate_90d_pct,
        
        
        LEAST(100, ROUND(
            (avg_customer_revenue / 1000 * 20) +  
            (avg_orders_per_customer * 10) +      
            (LEAST(5, avg_customer_lifespan_days / 30) * 10) +  
            ((100 - avg_refund_rate_pct) / 10)    
        , 0)) as customer_quality_score,
        
        CURRENT_TIMESTAMP as mart_created_at
        
    FROM segment_aggregates
)

SELECT * FROM final
WHERE customer_count > 0 
ORDER BY segment_revenue DESC