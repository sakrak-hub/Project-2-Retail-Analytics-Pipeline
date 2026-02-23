{{
    config(
        materialized='table',
        tags=['mart', 'aggregated', 'executive', 'kpi', 'business']
    )
}}

WITH date_spine AS (
    SELECT DISTINCT
        date_actual,
        date_key,
        year,
        quarter,
        month,
        month_name,
        week_of_year,
        is_weekend
    FROM {{ ref('dim_date') }}
    WHERE date_actual >= '2024-01-01'  
),

daily_summary AS (
    SELECT 
        d.date_actual,
        d.date_key,
        d.year,
        d.quarter,
        d.month,
        d.month_name,
        d.week_of_year,
        
        
        COALESCE(SUM(f.line_total) FILTER (WHERE f.is_refund = FALSE), 0) as gross_revenue,
        COALESCE(SUM(f.line_total - f.discount_amount) FILTER (WHERE f.is_refund = FALSE), 0) as net_revenue,
        COALESCE(SUM(f.line_profit) FILTER (WHERE f.is_refund = FALSE), 0) as gross_profit,
        COALESCE(SUM(f.total_cost) FILTER (WHERE f.is_refund = FALSE), 0) as total_cogs,
        COALESCE(AVG(f.line_profit_margin_pct) FILTER (WHERE f.is_refund = FALSE), 0) as avg_margin_pct,
        
        
        COALESCE(COUNT(DISTINCT f.sales_key) FILTER (WHERE f.is_refund = FALSE), 0) as total_orders,
        COALESCE(COUNT(DISTINCT f.transaction_id) FILTER (WHERE f.is_refund = FALSE), 0) as total_transactions,
        COALESCE(SUM(f.quantity) FILTER (WHERE f.is_refund = FALSE), 0) as total_units_sold,
        
        
        COALESCE(COUNT(DISTINCT f.customer_key) FILTER (WHERE f.is_refund = FALSE), 0) as unique_customers,
        COALESCE(COUNT(DISTINCT f.product_key) FILTER (WHERE f.is_refund = FALSE), 0) as unique_products_sold,
        COALESCE(COUNT(DISTINCT f.store_key) FILTER (WHERE f.is_refund = FALSE), 0) as active_stores,
        
        
        COALESCE(AVG(f.line_total) FILTER (WHERE f.is_refund = FALSE), 0) as avg_order_value,
        COALESCE(AVG(f.quantity) FILTER (WHERE f.is_refund = FALSE), 0) as avg_items_per_order,
        
        
        COALESCE(SUM(f.discount_amount) FILTER (WHERE f.is_refund = FALSE), 0) as total_discounts,
        COALESCE(COUNT(DISTINCT f.sales_key) FILTER (WHERE f.has_promotion = TRUE AND f.is_refund = FALSE), 0) as promotional_orders,
        COALESCE(SUM(f.line_total) FILTER (WHERE f.has_promotion = TRUE AND f.is_refund = FALSE), 0) as promotional_revenue,
        
        
        COALESCE(COUNT(DISTINCT f.sales_key) FILTER (WHERE f.is_refund = TRUE), 0) as refund_count,
        COALESCE(SUM(f.line_total) FILTER (WHERE f.is_refund = TRUE), 0) as refund_amount,
        
        
        COALESCE(SUM(f.loyalty_points_earned) FILTER (WHERE f.is_refund = FALSE), 0) as loyalty_points_issued
        
    FROM date_spine d
    LEFT JOIN {{ ref('fact_sales') }} f
        ON d.date_key = f.date_key
    
    GROUP BY d.date_actual, d.date_key, d.year, d.quarter, d.month, d.month_name, d.week_of_year
),

period_aggregates AS (
    SELECT 
        date_actual,
        year,
        quarter,
        month,
        month_name,
        week_of_year,
        
        
        gross_revenue,
        net_revenue,
        gross_profit,
        total_cogs,
        total_orders,
        unique_customers,
        avg_order_value,
        
        
        ROUND(gross_profit / NULLIF(gross_revenue, 0) * 100, 2) as gross_margin_pct,
        ROUND(total_discounts / NULLIF(gross_revenue, 0) * 100, 2) as discount_rate_pct,
        ROUND(refund_amount / NULLIF(gross_revenue, 0) * 100, 2) as refund_rate_pct,
        ROUND(promotional_revenue / NULLIF(gross_revenue, 0) * 100, 2) as promotional_mix_pct,
        ROUND(gross_revenue / NULLIF(unique_customers, 0), 2) as revenue_per_customer,
        ROUND(total_units_sold * 1.0 / NULLIF(total_orders, 0), 2) as units_per_order,
        
        
        
        LAG(gross_revenue, 1) OVER (ORDER BY date_actual) as prev_day_revenue,
        ROUND((gross_revenue - LAG(gross_revenue, 1) OVER (ORDER BY date_actual)) / 
              NULLIF(LAG(gross_revenue, 1) OVER (ORDER BY date_actual), 0) * 100, 2) as dod_revenue_change_pct,
        
        
        LAG(gross_revenue, 7) OVER (ORDER BY date_actual) as prev_week_revenue,
        ROUND((gross_revenue - LAG(gross_revenue, 7) OVER (ORDER BY date_actual)) / 
              NULLIF(LAG(gross_revenue, 7) OVER (ORDER BY date_actual), 0) * 100, 2) as wow_revenue_change_pct,
        
        
        LAG(gross_revenue, 30) OVER (ORDER BY date_actual) as prev_month_revenue,
        ROUND((gross_revenue - LAG(gross_revenue, 30) OVER (ORDER BY date_actual)) / 
              NULLIF(LAG(gross_revenue, 30) OVER (ORDER BY date_actual), 0) * 100, 2) as mom_revenue_change_pct,
        
        
        LAG(gross_revenue, 365) OVER (ORDER BY date_actual) as prev_year_revenue,
        ROUND((gross_revenue - LAG(gross_revenue, 365) OVER (ORDER BY date_actual)) / 
              NULLIF(LAG(gross_revenue, 365) OVER (ORDER BY date_actual), 0) * 100, 2) as yoy_revenue_change_pct,
        
        
        ROUND(AVG(gross_revenue) OVER (ORDER BY date_actual ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) as rolling_7day_avg_revenue,
        ROUND(AVG(gross_revenue) OVER (ORDER BY date_actual ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) as rolling_30day_avg_revenue,
        ROUND(AVG(total_orders) OVER (ORDER BY date_actual ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) as rolling_7day_avg_orders,
        
        
        SUM(gross_revenue) OVER (PARTITION BY year, month ORDER BY date_actual) as mtd_revenue,
        SUM(gross_revenue) OVER (PARTITION BY year, quarter ORDER BY date_actual) as qtd_revenue,
        SUM(gross_revenue) OVER (PARTITION BY year ORDER BY date_actual) as ytd_revenue,
        
        SUM(gross_profit) OVER (PARTITION BY year, month ORDER BY date_actual) as mtd_profit,
        SUM(gross_profit) OVER (PARTITION BY year, quarter ORDER BY date_actual) as qtd_profit,
        SUM(gross_profit) OVER (PARTITION BY year ORDER BY date_actual) as ytd_profit,
        
        SUM(total_orders) OVER (PARTITION BY year, month ORDER BY date_actual) as mtd_orders,
        SUM(total_orders) OVER (PARTITION BY year, quarter ORDER BY date_actual) as qtd_orders,
        SUM(total_orders) OVER (PARTITION BY year ORDER BY date_actual) as ytd_orders,
        
        
        total_discounts,
        refund_count,
        refund_amount,
        promotional_orders,
        promotional_revenue,
        total_units_sold,
        unique_products_sold,
        active_stores,
        loyalty_points_issued
        
    FROM daily_summary
),

final AS (
    SELECT 
        
        date_actual,
        year,
        'Q' || quarter as quarter_name,
        month,
        month_name,
        'W' || week_of_year as week_name,
        CASE WHEN DAYOFWEEK(date_actual) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend,
        
        
        CASE WHEN date_actual = CURRENT_DATE THEN TRUE ELSE FALSE END as is_today,
        CASE WHEN date_actual = CURRENT_DATE - INTERVAL 1 DAY THEN TRUE ELSE FALSE END as is_yesterday,
        CASE WHEN date_actual >= CURRENT_DATE - INTERVAL 7 DAY THEN TRUE ELSE FALSE END as is_last_7_days,
        CASE WHEN date_actual >= CURRENT_DATE - INTERVAL 30 DAY THEN TRUE ELSE FALSE END as is_last_30_days,
        CASE WHEN date_actual >= CURRENT_DATE - INTERVAL 90 DAY THEN TRUE ELSE FALSE END as is_last_90_days,
        
        
        gross_revenue,
        net_revenue,
        gross_profit,
        total_cogs,
        gross_margin_pct,
        
        
        total_orders,
        total_units_sold,
        unique_customers,
        unique_products_sold,
        active_stores,
        
        
        avg_order_value,
        revenue_per_customer,
        units_per_order,
        
        
        total_discounts,
        discount_rate_pct,
        promotional_orders,
        promotional_revenue,
        promotional_mix_pct,
        
        
        refund_count,
        refund_amount,
        refund_rate_pct,
        
        
        loyalty_points_issued,
        
        
        prev_day_revenue,
        dod_revenue_change_pct,
        prev_week_revenue,
        wow_revenue_change_pct,
        prev_month_revenue,
        mom_revenue_change_pct,
        prev_year_revenue,
        yoy_revenue_change_pct,
        
        
        rolling_7day_avg_revenue,
        rolling_30day_avg_revenue,
        rolling_7day_avg_orders,
        
        
        mtd_revenue,
        mtd_profit,
        mtd_orders,
        qtd_revenue,
        qtd_profit,
        qtd_orders,
        ytd_revenue,
        ytd_profit,
        ytd_orders,
        
        
        CASE 
            WHEN gross_margin_pct >= 40 THEN 'Excellent'
            WHEN gross_margin_pct >= 30 THEN 'Good'
            WHEN gross_margin_pct >= 20 THEN 'Fair'
            ELSE 'Poor'
        END as margin_performance,
        
        CASE 
            WHEN refund_rate_pct <= 2 THEN 'Excellent'
            WHEN refund_rate_pct <= 5 THEN 'Good'
            WHEN refund_rate_pct <= 10 THEN 'Fair'
            ELSE 'Poor'
        END as quality_indicator,
        
        CASE 
            WHEN dod_revenue_change_pct > 10 THEN 'Strong Growth'
            WHEN dod_revenue_change_pct > 0 THEN 'Growth'
            WHEN dod_revenue_change_pct >= -5 THEN 'Stable'
            ELSE 'Declining'
        END as revenue_trend,
        
        CURRENT_TIMESTAMP as mart_created_at
        
    FROM period_aggregates
)

SELECT * FROM final
ORDER BY date_actual DESC