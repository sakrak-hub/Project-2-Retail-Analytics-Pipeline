{{
    config(
        materialized='table',
        tags=['mart', 'aggregated', 'product', 'business']
    )
}}

WITH product_sales AS (
    SELECT 
        
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.supplier,
        p.selling_price,
        p.cost_price,
        p.profit_margin_pct as product_margin_pct,
        p.price_tier,
        p.price_category,
        p.stock_quantity,
        p.stock_status,
        p.is_available,
        p.launch_date,
        p.product_maturity,
        
        
        COUNT(DISTINCT f.sales_key) as total_orders,
        COUNT(DISTINCT f.transaction_id) as total_transactions,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        SUM(f.quantity) as total_units_sold,
        
        
        SUM(f.line_total) as total_revenue,
        SUM(f.gross_amount) as gross_revenue,
        SUM(f.discount_amount) as total_discounts,
        AVG(f.unit_price) as avg_selling_price,
        
        
        SUM(f.total_cost) as total_cost,
        SUM(f.line_profit) as total_profit,
        AVG(f.line_profit_margin_pct) as realized_margin_pct,
        
        
        AVG(f.quantity) as avg_quantity_per_order,
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.is_bulk_purchase = TRUE) as bulk_orders,
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.has_discount = TRUE) as discounted_orders,
        
        
        COUNT(DISTINCT f.sales_key) FILTER (WHERE f.is_refund = TRUE) as refund_count,
        SUM(f.line_total) FILTER (WHERE f.is_refund = TRUE) as refund_amount,
        
        
        MIN(f.transaction_date) as first_sale_date,
        MAX(f.transaction_date) as last_sale_date,
        COUNT(DISTINCT DATE(f.transaction_date)) as active_days,
        
        
        SUM(f.quantity) / NULLIF(p.stock_quantity, 0) as stock_turnover_ratio
        
    FROM {{ ref('fact_sales') }} f
    INNER JOIN {{ ref('dim_products') }} p 
        ON f.product_key = p.product_key
        AND p.is_current = TRUE  
    
    WHERE f.is_refund = FALSE  
    
    GROUP BY 
        p.product_id, p.product_name, p.category, p.subcategory, p.brand, p.supplier,
        p.selling_price, p.cost_price, p.profit_margin_pct, p.price_tier, p.price_category,
        p.stock_quantity, p.stock_status, p.is_available, p.launch_date, p.product_maturity
),

product_ranking AS (
    SELECT 
        *,
        
        
        ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as revenue_rank,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY total_revenue DESC) as category_revenue_rank,
        
        
        ROW_NUMBER() OVER (ORDER BY total_profit DESC) as profit_rank,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY total_profit DESC) as category_profit_rank,
        
        
        ROW_NUMBER() OVER (ORDER BY total_units_sold DESC) as units_sold_rank,
        
        
        NTILE(10) OVER (ORDER BY total_revenue) as revenue_decile,
        NTILE(4) OVER (ORDER BY total_revenue) as revenue_quartile
        
    FROM product_sales
),

final AS (
    SELECT 
        *,
        
        
        ROUND(total_profit / NULLIF(total_revenue, 0) * 100, 2) as profit_margin_pct,
        ROUND(total_discounts / NULLIF(gross_revenue, 0) * 100, 2) as discount_rate_pct,
        ROUND(refund_amount / NULLIF(total_revenue, 0) * 100, 2) as refund_rate_pct,
        ROUND(discounted_orders * 100.0 / NULLIF(total_orders, 0), 2) as discount_frequency_pct,
        ROUND(total_revenue / NULLIF(total_units_sold, 0), 2) as revenue_per_unit,
        ROUND(total_revenue / NULLIF(unique_customers, 0), 2) as revenue_per_customer,
        ROUND(total_units_sold * 1.0 / NULLIF(active_days, 0), 2) as avg_daily_sales,
        
        
        DATEDIFF('day', last_sale_date, CURRENT_DATE) as days_since_last_sale,
        
        
        CASE 
            WHEN days_since_last_sale <= 7 THEN 'Active'
            WHEN days_since_last_sale <= 30 THEN 'Recent'
            WHEN days_since_last_sale <= 90 THEN 'Slow'
            ELSE 'Stale'
        END as sales_velocity,
        
        CASE 
            WHEN revenue_decile >= 9 THEN 'Top 20%'
            WHEN revenue_decile >= 7 THEN 'Top 40%'
            WHEN revenue_decile >= 5 THEN 'Middle 40%'
            ELSE 'Bottom 40%'
        END as revenue_tier,
        
        CASE 
            WHEN total_profit > 0 AND realized_margin_pct > 30 THEN 'High Margin Winner'
            WHEN total_profit > 0 AND total_revenue > 1000 THEN 'Volume Driver'
            WHEN total_profit > 0 THEN 'Profitable'
            WHEN total_profit = 0 THEN 'Break Even'
            ELSE 'Loss Leader'
        END as profitability_status,
        
        
        CASE 
            WHEN revenue_decile >= 8 THEN 'A - Top Performers'
            WHEN revenue_decile >= 5 THEN 'B - Steady Performers'
            ELSE 'C - Low Performers'
        END as abc_classification,
        
        
        CASE 
            WHEN stock_quantity = 0 THEN 'Out of Stock'
            WHEN stock_turnover_ratio > 5 THEN 'Fast Moving'
            WHEN stock_turnover_ratio > 2 THEN 'Moderate'
            WHEN stock_turnover_ratio > 0 THEN 'Slow Moving'
            ELSE 'No Movement'
        END as inventory_status,
        
        CURRENT_TIMESTAMP as mart_created_at
        
    FROM product_ranking
)

SELECT * FROM final
ORDER BY total_revenue DESC
