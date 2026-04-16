{{
    config(
        materialized='table',
        tags=['mart', 'simple', 'revenue']
    )
}}




WITH daily_metrics AS (
    SELECT 
        
        transaction_date::DATE as date,
        YEAR(transaction_date) as year,
        QUARTER(transaction_date) as quarter,
        MONTH(transaction_date) as month,
        MONTHNAME(transaction_date) as month_name,
        DAYNAME(transaction_date) as day_name,
        CASE WHEN DAYOFWEEK(transaction_date) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END as day_type,
        
        
        SUM(line_total) as revenue,
        SUM(total_cost) as cost,
        SUM(line_profit) as profit,
        
        
        COUNT(DISTINCT transaction_id) as transactions,
        COUNT(DISTINCT customer_key) as customers,
        SUM(quantity) as units_sold,
        
        
        AVG(line_total) as avg_order_value
        
    FROM {{ ref('fact_sales') }}
    WHERE is_refund = FALSE
    GROUP BY 
        transaction_date::DATE,
        YEAR(transaction_date),
        QUARTER(transaction_date),
        MONTH(transaction_date),
        MONTHNAME(transaction_date),
        DAYNAME(transaction_date),
        CASE WHEN DAYOFWEEK(transaction_date) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END
)

SELECT 
    
    date,
    year,
    quarter,
    month,
    month_name,
    day_name,
    day_type,
    
    
    ROUND(revenue, 2) as revenue,
    ROUND(cost, 2) as cost,
    ROUND(profit, 2) as profit,
    ROUND(profit / NULLIF(revenue, 0) * 100, 1) as profit_margin_pct,
    
    
    transactions,
    customers,
    units_sold,
    ROUND(avg_order_value, 2) as avg_order_value,
    
    
    ROUND(revenue - LAG(revenue, 1) OVER (ORDER BY date), 2) as revenue_change_vs_yesterday,
    ROUND(revenue - LAG(revenue, 7) OVER (ORDER BY date), 2) as revenue_change_vs_last_week,
    
    
    ROUND(AVG(revenue) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) as revenue_7day_avg,
    ROUND(AVG(revenue) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) as revenue_30day_avg,
    
    
    SUM(revenue) OVER (PARTITION BY year, month ORDER BY date) as revenue_mtd,
    SUM(revenue) OVER (PARTITION BY year ORDER BY date) as revenue_ytd,
    
    
    CASE WHEN date = CURRENT_DATE THEN TRUE ELSE FALSE END as is_today,
    CASE WHEN date >= CURRENT_DATE - INTERVAL 7 DAY THEN TRUE ELSE FALSE END as is_last_7_days,
    CASE WHEN date >= CURRENT_DATE - INTERVAL 30 DAY THEN TRUE ELSE FALSE END as is_last_30_days
    
FROM daily_metrics
ORDER BY date DESC
