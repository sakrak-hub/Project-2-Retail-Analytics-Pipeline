{{
    config(
        materialized='table',
        tags=['mart', 'store']
    )
}}

WITH store_metrics AS (
    SELECT 
        
        s.store_id,
        s.store_name,
        s.city,
        s.state,
        s.region,
        s.manager,
        
        
        SUM(f.line_total) as total_revenue,
        SUM(f.line_profit) as total_profit,
        COUNT(DISTINCT f.transaction_id) as total_transactions,
        COUNT(DISTINCT f.customer_key) as total_customers,
        SUM(f.quantity) as total_units_sold,
        
        
        AVG(f.line_total) as avg_transaction_value,
        AVG(f.line_profit_margin_pct) as avg_profit_margin_pct,
        
        
        MIN(f.transaction_date) as first_sale_date,
        MAX(f.transaction_date) as last_sale_date,
        COUNT(DISTINCT f.transaction_date::DATE) as days_with_sales
        
    FROM {{ ref('fact_sales') }} f
    INNER JOIN {{ ref('dim_stores') }} s 
        ON f.store_key = s.store_key
        AND s.is_current = TRUE
    WHERE f.is_refund = FALSE
    GROUP BY 
        s.store_id, s.store_name, s.city, s.state, s.region, s.manager
)

SELECT 
    
    store_id,
    store_name,
    city,
    state,
    region,
    manager,
    
    ROUND(total_revenue, 2) as total_revenue,
    ROUND(total_profit, 2) as total_profit,
    ROUND(total_profit / NULLIF(total_revenue, 0) * 100, 1) as profit_margin_pct,
    
    total_transactions,
    total_customers,
    total_units_sold,
    
    ROUND(total_revenue / NULLIF(days_with_sales, 0), 2) as avg_daily_revenue,
    ROUND(total_transactions * 1.0 / NULLIF(days_with_sales, 0), 1) as avg_daily_transactions,
    
    ROUND(total_revenue / NULLIF(total_customers, 0), 2) as revenue_per_customer,
    ROUND(total_transactions * 1.0 / NULLIF(total_customers, 0), 1) as transactions_per_customer,
    
    ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as revenue_rank,
    ROW_NUMBER() OVER (ORDER BY total_profit DESC) as profit_rank,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_revenue DESC) as rank_in_region,
    
    CASE 
        WHEN ROW_NUMBER() OVER (ORDER BY total_revenue DESC) <= 3 THEN 'Top 3'
        WHEN NTILE(4) OVER (ORDER BY total_revenue) = 4 THEN 'Top 25%'
        WHEN NTILE(4) OVER (ORDER BY total_revenue) = 3 THEN 'Above Average'
        WHEN NTILE(4) OVER (ORDER BY total_revenue) = 2 THEN 'Below Average'
        ELSE 'Bottom 25%'
    END as performance_tier,
    
    CASE 
        WHEN avg_profit_margin_pct >= 30 THEN 'Excellent Margin'
        WHEN avg_profit_margin_pct >= 20 THEN 'Good Margin'
        WHEN avg_profit_margin_pct >= 10 THEN 'Fair Margin'
        ELSE 'Low Margin'
    END as margin_status,
    
    first_sale_date,
    last_sale_date,
    days_with_sales,
    DATEDIFF('day', last_sale_date, CURRENT_DATE) as days_since_last_sale
    
FROM store_metrics
ORDER BY total_revenue DESC
