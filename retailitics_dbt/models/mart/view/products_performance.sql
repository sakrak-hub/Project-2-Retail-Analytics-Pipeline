{{
    config(
        materialized='table',
        tags=['mart', 'simple', 'products']
    )
}}

WITH product_metrics AS (
    SELECT 
        
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.selling_price,
        p.cost_price,
        
        SUM(f.line_total) as total_revenue,
        SUM(f.line_profit) as total_profit,
        SUM(f.quantity) as total_units_sold,
        COUNT(DISTINCT f.transaction_id) as total_transactions,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        
        AVG(f.unit_price) as avg_selling_price,
        AVG(f.line_profit_margin_pct) as avg_profit_margin_pct,
        
        MIN(f.transaction_date) as first_sale_date,
        MAX(f.transaction_date) as last_sale_date,
        COUNT(DISTINCT f.transaction_date::DATE) as days_with_sales
        
    FROM {{ ref('fact_sales') }} f
    INNER JOIN {{ ref('dim_products') }} p 
        ON f.product_key = p.product_key
        AND p.is_current = TRUE
    WHERE f.is_refund = FALSE
    GROUP BY 
        p.product_id, p.product_name, p.category, p.subcategory, 
        p.brand, p.selling_price, p.cost_price
)

SELECT 
    
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    
    ROUND(selling_price, 2) as list_price,
    ROUND(cost_price, 2) as cost_price,
    ROUND(avg_selling_price, 2) as avg_actual_price,
    
    ROUND(total_revenue, 2) as total_revenue,
    ROUND(total_profit, 2) as total_profit,
    ROUND(total_profit / NULLIF(total_revenue, 0) * 100, 1) as profit_margin_pct,
    
    total_units_sold,
    total_transactions,
    unique_customers,
    
    ROUND(total_revenue / NULLIF(days_with_sales, 0), 2) as avg_daily_revenue,
    ROUND(total_units_sold * 1.0 / NULLIF(days_with_sales, 0), 1) as avg_daily_units,
    
    ROUND(total_revenue / NULLIF(unique_customers, 0), 2) as revenue_per_customer,
    ROUND(total_units_sold * 1.0 / NULLIF(unique_customers, 0), 1) as units_per_customer,
    
    ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as revenue_rank,
    ROW_NUMBER() OVER (ORDER BY total_profit DESC) as profit_rank,
    ROW_NUMBER() OVER (ORDER BY total_units_sold DESC) as units_rank,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY total_revenue DESC) as rank_in_category,
    
    CASE 
        WHEN ROW_NUMBER() OVER (ORDER BY total_revenue DESC) <= 10 THEN 'Top 10'
        WHEN NTILE(100) OVER (ORDER BY total_revenue) >= 80 THEN 'Top 20%'
        WHEN NTILE(100) OVER (ORDER BY total_revenue) >= 50 THEN 'Top 50%'
        ELSE 'Bottom 50%'
    END as performance_tier,
    
    CASE 
        WHEN SUM(total_revenue) OVER (ORDER BY total_revenue DESC ROWS UNBOUNDED PRECEDING) 
             <= SUM(total_revenue) OVER () * 0.8 THEN 'A - Top Products'
        WHEN SUM(total_revenue) OVER (ORDER BY total_revenue DESC ROWS UNBOUNDED PRECEDING) 
             <= SUM(total_revenue) OVER () * 0.95 THEN 'B - Medium Products'
        ELSE 'C - Low Products'
    END as abc_class,
    
    CASE 
        WHEN total_profit > 0 AND avg_profit_margin_pct > 30 THEN 'High Margin Winner'
        WHEN total_profit > 0 AND total_revenue > 1000 THEN 'Volume Driver'
        WHEN total_profit > 0 THEN 'Profitable'
        ELSE 'Loss Maker'
    END as profit_status,
    
    CASE 
        WHEN DATEDIFF('day', last_sale_date, CURRENT_DATE) <= 7 THEN 'Hot Seller'
        WHEN DATEDIFF('day', last_sale_date, CURRENT_DATE) <= 30 THEN 'Active'
        WHEN DATEDIFF('day', last_sale_date, CURRENT_DATE) <= 90 THEN 'Slow Moving'
        ELSE 'Dormant'
    END as sales_velocity,
    
    first_sale_date,
    last_sale_date,
    days_with_sales,
    DATEDIFF('day', last_sale_date, CURRENT_DATE) as days_since_last_sale
    
FROM product_metrics
ORDER BY total_revenue DESC
