{{
    config(
        materialized='table',
        tags=['mart', 'aggregated', 'daily', 'business']
    )
}}

WITH daily_metrics AS (
    SELECT 
        d.date_actual,
        d.date_key,
        d.year,
        d.quarter,
        d.month,
        d.month_name,
        d.week_of_year,
        d.day_of_week,
        d.day_name,
        d.is_weekend,
        d.is_holiday,
        d.holiday_name,
        
        COUNT(DISTINCT f.sales_key) as total_orders,
        COUNT(DISTINCT f.transaction_id) as total_transactions,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        SUM(f.quantity) as total_units_sold,
        SUM(f.items_count) as total_items,
        
        SUM(f.line_total) as gross_revenue,
        SUM(f.discount_amount) as total_discounts,
        SUM(f.net_amount) as net_revenue,
        SUM(f.tax_amount) as total_tax,
        
        SUM(f.total_cost) as total_cost,
        SUM(f.line_profit) as total_profit,
        AVG(f.line_profit_margin_pct) as avg_profit_margin_pct,
        
        AVG(f.line_total) as avg_order_value,
        AVG(f.quantity) as avg_items_per_order,
        SUM(f.line_total) / NULLIF(COUNT(DISTINCT f.customer_key), 0) as revenue_per_customer,
        
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.has_promotion = TRUE) as promotional_orders,
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.has_discount = TRUE) as discounted_orders,
        SUM(f.line_total) FILTER (WHERE f.has_promotion = TRUE) as promotional_revenue,
        
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.is_refund = TRUE) as refund_count,
        SUM(f.line_total) FILTER (WHERE f.is_refund = TRUE) as refund_amount,
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.is_refund = TRUE) * 100.0 / 
            NULLIF(COUNT(DISTINCT f.sales_key), 0) as refund_rate_pct,
        
        SUM(f.loyalty_points_earned) as total_loyalty_points,
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.earned_loyalty_points = TRUE) as loyalty_transactions,
        
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.payment_method = 'Credit Card') as credit_card_orders,
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.payment_method = 'Cash') as cash_orders,
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.payment_method = 'Debit Card') as debit_orders,
        
        SUM(f.line_total) FILTER (WHERE f.time_of_day = 'Morning (6-11am)') as morning_revenue,
        SUM(f.line_total) FILTER (WHERE f.time_of_day = 'Afternoon (12-4pm)') as afternoon_revenue,
        SUM(f.line_total) FILTER (WHERE f.time_of_day = 'Evening (5-8pm)') as evening_revenue,
        
        MIN(f.created_at) as earliest_load_time,
        MAX(f.created_at) as latest_load_time
        
    FROM {{ ref('fact_sales') }} f
    INNER JOIN {{ ref('dim_date') }} d 
        ON f.date_key = d.date_key
    
    GROUP BY 
        d.date_actual, d.date_key, d.year, d.quarter, d.month, d.month_name,
        d.week_of_year, d.day_of_week, d.day_name, d.is_weekend, 
        d.is_holiday, d.holiday_name
),

final AS (
    SELECT 
        *,
        
        ROUND(total_profit / NULLIF(gross_revenue, 0) * 100, 2) as profit_margin_pct,
        ROUND(total_discounts / NULLIF(gross_revenue, 0) * 100, 2) as discount_rate_pct,
        ROUND(promotional_revenue / NULLIF(gross_revenue, 0) * 100, 2) as promotional_mix_pct,
        ROUND(total_units_sold * 1.0 / NULLIF(total_transactions, 0), 2) as units_per_transaction,
        
        CASE WHEN date_actual = CURRENT_DATE THEN TRUE ELSE FALSE END as is_today,
        CASE WHEN date_actual = CURRENT_DATE - INTERVAL 1 DAY THEN TRUE ELSE FALSE END as is_yesterday,
        CASE WHEN date_actual >= CURRENT_DATE - INTERVAL 7 DAY THEN TRUE ELSE FALSE END as is_last_7_days,
        CASE WHEN date_actual >= CURRENT_DATE - INTERVAL 30 DAY THEN TRUE ELSE FALSE END as is_last_30_days,
        CASE WHEN date_actual >= CURRENT_DATE - INTERVAL 90 DAY THEN TRUE ELSE FALSE END as is_last_90_days,
        
        CURRENT_TIMESTAMP as mart_created_at
        
    FROM daily_metrics
)

SELECT * FROM final
ORDER BY date_actual DESC
