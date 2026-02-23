{{
    config(
        materialized='table',
        tags=['mart', 'aggregated', 'store', 'business']
    )
}}

WITH store_sales AS (
    SELECT 
        
        s.store_id,
        s.store_name,
        s.store_type,
        s.store_category,
        s.manager,
        s.city,
        s.state,
        s.region,
        s.is_metro_area,
        s.opening_date,
        s.store_maturity_tier,
        s.is_fully_operational,
        s.is_legacy_store,
        DATEDIFF('day', s.opening_date, CURRENT_DATE) as days_operational,
        
        
        COUNT(DISTINCT f.sales_key) as total_orders,
        COUNT(DISTINCT f.transaction_id) as total_transactions,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        COUNT(DISTINCT DATE(f.transaction_date)) as active_days,
        SUM(f.quantity) as total_units_sold,
        SUM(f.items_count) as total_items,
        
        
        SUM(f.line_total) as total_revenue,
        SUM(f.discount_amount) as total_discounts,
        SUM(f.tax_amount) as total_tax,
        AVG(f.line_total) as avg_order_value,
        
        
        SUM(f.total_cost) as total_cost,
        SUM(f.line_profit) as total_profit,
        AVG(f.line_profit_margin_pct) as avg_margin_pct,
        
        
        COUNT(DISTINCT f.sales_key) / NULLIF(COUNT(DISTINCT f.customer_key), 0) as avg_orders_per_customer,
        SUM(f.line_total) / NULLIF(COUNT(DISTINCT f.customer_key), 0) as revenue_per_customer,
        
        
        COUNT(DISTINCT f.product_key) as unique_products_sold,
        AVG(f.quantity) as avg_items_per_order,
        
        
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.has_promotion = TRUE) as promotional_orders,
        SUM(f.line_total) FILTER (WHERE f.has_promotion = TRUE) as promotional_revenue,
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.has_discount = TRUE) as discounted_orders,
        
        
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.payment_method = 'Credit Card') as credit_card_txns,
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.payment_method = 'Cash') as cash_txns,
        SUM(f.line_total) FILTER (WHERE f.payment_method = 'Cash') as cash_revenue,
        
        
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.is_refund = TRUE) as refund_count,
        SUM(f.line_total) FILTER (WHERE f.is_refund = TRUE) as refund_amount,
        
        
        SUM(f.loyalty_points_earned) as total_loyalty_points,
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.earned_loyalty_points = TRUE) as loyalty_transactions,
        
        
        SUM(f.line_total) FILTER (WHERE f.time_of_day = 'Morning (6-11am)') as morning_revenue,
        SUM(f.line_total) FILTER (WHERE f.time_of_day = 'Afternoon (12-4pm)') as afternoon_revenue,
        SUM(f.line_total) FILTER (WHERE f.time_of_day = 'Evening (5-8pm)') as evening_revenue,
        
        
        SUM(f.line_total) FILTER (WHERE f.is_weekend = TRUE) as weekend_revenue,
        SUM(f.line_total) FILTER (WHERE f.is_weekend = FALSE) as weekday_revenue,
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.is_weekend = TRUE) as weekend_orders,
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.is_weekend = FALSE) as weekday_orders
        
    FROM {{ ref('fact_sales') }} f
    INNER JOIN {{ ref('dim_stores') }} s 
        ON f.store_key = s.store_key
        AND s.is_current = TRUE  
    
    WHERE f.is_refund = FALSE  
    
    GROUP BY 
        s.store_id, s.store_name, s.store_type, s.store_category, s.manager,
        s.city, s.state, s.region, s.is_metro_area, s.opening_date,
        s.store_maturity_tier, s.is_fully_operational, s.is_legacy_store
),

store_ranking AS (
    SELECT 
        *,
        
        ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as revenue_rank,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_revenue DESC) as region_revenue_rank,
        ROW_NUMBER() OVER (PARTITION BY store_type ORDER BY total_revenue DESC) as type_revenue_rank,
        
        
        ROW_NUMBER() OVER (ORDER BY total_profit DESC) as profit_rank,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_profit DESC) as region_profit_rank,
        
        
        ROW_NUMBER() OVER (ORDER BY revenue_per_customer DESC) as efficiency_rank,
        
        
        NTILE(10) OVER (ORDER BY total_revenue) as revenue_decile,
        NTILE(4) OVER (ORDER BY total_revenue) as revenue_quartile
        
    FROM store_sales
),


product_stats AS (
    SELECT COUNT(DISTINCT product_id) as total_product_count
    FROM {{ ref('dim_products') }}
    WHERE is_current = TRUE
),

final AS (
    SELECT 
        sr.*,
        
        
        ROUND(sr.total_revenue / NULLIF(sr.active_days, 0), 2) as avg_daily_revenue,
        ROUND(sr.total_revenue / NULLIF(sr.days_operational, 0) * 365, 2) as annualized_revenue,
        ROUND(sr.total_profit / NULLIF(sr.total_revenue, 0) * 100, 2) as profit_margin_pct,
        ROUND(sr.total_discounts / NULLIF(sr.total_revenue, 0) * 100, 2) as discount_rate_pct,
        ROUND(sr.refund_amount / NULLIF(sr.total_revenue, 0) * 100, 2) as refund_rate_pct,
        
        
        ROUND(sr.unique_customers * 1.0 / NULLIF(sr.active_days, 0), 2) as avg_daily_customers,
        ROUND(sr.total_orders * 1.0 / NULLIF(sr.active_days, 0), 2) as avg_daily_orders,
        
        
        ROUND(sr.unique_products_sold * 100.0 / NULLIF(ps.total_product_count, 0), 2) as product_coverage_pct,
        
        
        ROUND(sr.promotional_revenue / NULLIF(sr.total_revenue, 0) * 100, 2) as promotional_mix_pct,
        ROUND(sr.discounted_orders * 100.0 / NULLIF(sr.total_orders, 0), 2) as discount_frequency_pct,
        
        
        ROUND(sr.cash_revenue / NULLIF(sr.total_revenue, 0) * 100, 2) as cash_mix_pct,
        ROUND(sr.credit_card_txns * 100.0 / NULLIF(sr.total_transactions, 0), 2) as credit_card_rate_pct,
        
        
        ROUND(sr.loyalty_transactions * 100.0 / NULLIF(sr.total_transactions, 0), 2) as loyalty_participation_pct,
        
        
        ROUND(sr.weekend_revenue / NULLIF(sr.total_revenue, 0) * 100, 2) as weekend_revenue_pct,
        ROUND((sr.morning_revenue + sr.afternoon_revenue + sr.evening_revenue) / NULLIF(sr.total_revenue, 0) * 100, 2) as core_hours_pct,
        
        
        CASE 
            WHEN sr.revenue_decile >= 9 THEN 'Top Tier (Top 20%)'
            WHEN sr.revenue_decile >= 7 THEN 'High Performers (60-80%)'
            WHEN sr.revenue_decile >= 5 THEN 'Average Performers (40-60%)'
            WHEN sr.revenue_decile >= 3 THEN 'Below Average (20-40%)'
            ELSE 'Bottom Tier (Bottom 20%)'
        END as performance_tier,
        
        
        LEAST(100, ROUND(
            (LEAST(5000, sr.total_revenue) / 5000 * 30) +  
            (LEAST(25, sr.avg_margin_pct) / 25 * 25) +     
            (LEAST(100, sr.unique_customers) / 100 * 20) + 
            ((100 - ROUND(sr.refund_amount / NULLIF(sr.total_revenue, 0) * 100, 2)) / 100 * 15) +
            (LEAST(50, ROUND(sr.unique_products_sold * 100.0 / NULLIF(ps.total_product_count, 0), 2)) / 50 * 10)
        , 0)) as store_health_score,
        
        
        CASE 
            WHEN sr.is_metro_area = TRUE AND sr.revenue_decile < 7 THEN 'High Growth Potential'
            WHEN sr.is_metro_area = TRUE THEN 'Optimize Performance'
            WHEN sr.revenue_decile >= 7 THEN 'Strong Regional Performer'
            WHEN sr.days_operational < 365 THEN 'New Store - Monitor'
            ELSE 'Stable'
        END as growth_potential,
        
        
        CASE 
            WHEN sr.is_fully_operational = FALSE THEN 'Limited Operations'
            WHEN ROUND(sr.refund_amount / NULLIF(sr.total_revenue, 0) * 100, 2) > 10 THEN 'Quality Issues'
            WHEN sr.avg_margin_pct < 20 THEN 'Margin Pressure'
            WHEN LEAST(100, ROUND(
                (LEAST(5000, sr.total_revenue) / 5000 * 30) +  
                (LEAST(25, sr.avg_margin_pct) / 25 * 25) +     
                (LEAST(100, sr.unique_customers) / 100 * 20) + 
                ((100 - ROUND(sr.refund_amount / NULLIF(sr.total_revenue, 0) * 100, 2)) / 100 * 15) +
                (LEAST(50, ROUND(sr.unique_products_sold * 100.0 / NULLIF(ps.total_product_count, 0), 2)) / 50 * 10)
            , 0)) >= 80 THEN 'Excellent'
            WHEN LEAST(100, ROUND(
                (LEAST(5000, sr.total_revenue) / 5000 * 30) +  
                (LEAST(25, sr.avg_margin_pct) / 25 * 25) +     
                (LEAST(100, sr.unique_customers) / 100 * 20) + 
                ((100 - ROUND(sr.refund_amount / NULLIF(sr.total_revenue, 0) * 100, 2)) / 100 * 15) +
                (LEAST(50, ROUND(sr.unique_products_sold * 100.0 / NULLIF(ps.total_product_count, 0), 2)) / 50 * 10)
            , 0)) >= 60 THEN 'Good'
            ELSE 'Needs Improvement'
        END as operational_status,
        
        CURRENT_TIMESTAMP as mart_created_at
        
    FROM store_ranking sr
    CROSS JOIN product_stats ps  
)

SELECT * FROM final
ORDER BY total_revenue DESC