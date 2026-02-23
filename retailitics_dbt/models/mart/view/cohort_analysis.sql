{{
    config(
        materialized='table',
        tags=['mart', 'aggregated', 'cohort', 'business']
    )
}}

WITH customer_first_purchase AS (
    SELECT 
        f.customer_key,
        MIN(f.transaction_date) as first_purchase_date,
        DATE_TRUNC('month', MIN(f.transaction_date)) as cohort_month,
        YEAR(MIN(f.transaction_date)) as cohort_year,
        QUARTER(MIN(f.transaction_date)) as cohort_quarter
    FROM {{ ref('fact_sales') }} f
    WHERE f.is_refund = FALSE
    GROUP BY f.customer_key
),

customer_monthly_activity AS (
    SELECT 
        f.customer_key,
        DATE_TRUNC('month', f.transaction_date) as activity_month,
        COUNT(DISTINCT f.sales_key) as orders,
        SUM(f.line_total) as revenue,
        SUM(f.line_profit) as profit
    FROM {{ ref('fact_sales') }} f
    WHERE f.is_refund = FALSE
    GROUP BY f.customer_key, DATE_TRUNC('month', f.transaction_date)
),

cohort_activity AS (
    SELECT 
        cfp.cohort_month,
        cfp.cohort_year,
        cfp.cohort_quarter,
        cma.activity_month,
        DATEDIFF('month', cfp.cohort_month, cma.activity_month) as months_since_first_purchase,
        
        
        COUNT(DISTINCT cfp.customer_key) as cohort_size,
        COUNT(DISTINCT cma.customer_key) as active_customers,
        SUM(cma.orders) as total_orders,
        SUM(cma.revenue) as total_revenue,
        SUM(cma.profit) as total_profit,
        AVG(cma.revenue) as avg_revenue_per_customer,
        AVG(cma.orders) as avg_orders_per_customer
        
    FROM customer_first_purchase cfp
    LEFT JOIN customer_monthly_activity cma 
        ON cfp.customer_key = cma.customer_key
        AND cma.activity_month >= cfp.cohort_month
    
    GROUP BY cfp.cohort_month, cfp.cohort_year, cfp.cohort_quarter, 
             cma.activity_month, DATEDIFF('month', cfp.cohort_month, cma.activity_month)
),

cohort_base_metrics AS (
    SELECT 
        cohort_month,
        MAX(cohort_size) as initial_cohort_size,
        SUM(total_revenue) FILTER (WHERE months_since_first_purchase = 0) as month_0_revenue,
        SUM(total_orders) FILTER (WHERE months_since_first_purchase = 0) as month_0_orders
    FROM cohort_activity
    GROUP BY cohort_month
),

final AS (
    SELECT 
        
        ca.cohort_month,
        MONTHNAME(ca.cohort_month) as cohort_month_name,
        ca.cohort_year,
        'Q' || ca.cohort_quarter as cohort_quarter_name,
        
        
        ca.activity_month,
        MONTHNAME(ca.activity_month) as activity_month_name,
        ca.months_since_first_purchase,
        
        
        cbm.initial_cohort_size,
        ca.active_customers,
        
        
        ROUND(ca.active_customers * 100.0 / NULLIF(cbm.initial_cohort_size, 0), 2) as retention_rate_pct,
        ROUND((cbm.initial_cohort_size - ca.active_customers) * 100.0 / NULLIF(cbm.initial_cohort_size, 0), 2) as churn_rate_pct,
        
        
        ca.total_revenue,
        ca.total_profit,
        ca.avg_revenue_per_customer,
        ROUND(ca.total_revenue / NULLIF(cbm.initial_cohort_size, 0), 2) as revenue_per_original_customer,
        
        
        ca.total_orders,
        ca.avg_orders_per_customer,
        ROUND(ca.total_orders * 1.0 / NULLIF(cbm.initial_cohort_size, 0), 2) as orders_per_original_customer,
        
        
        SUM(ca.total_revenue) OVER (
            PARTITION BY ca.cohort_month 
            ORDER BY ca.activity_month
        ) as cumulative_revenue,
        
        SUM(ca.total_profit) OVER (
            PARTITION BY ca.cohort_month 
            ORDER BY ca.activity_month
        ) as cumulative_profit,
        
        
        ROUND(SUM(ca.total_revenue) OVER (
            PARTITION BY ca.cohort_month 
            ORDER BY ca.activity_month
        ) / NULLIF(cbm.initial_cohort_size, 0), 2) as customer_ltv_to_date,
        
        
        LAG(ca.active_customers) OVER (
            PARTITION BY ca.cohort_month 
            ORDER BY ca.activity_month
        ) as prev_month_active,
        
        ROUND((ca.active_customers - LAG(ca.active_customers) OVER (
            PARTITION BY ca.cohort_month 
            ORDER BY ca.activity_month
        )) * 100.0 / NULLIF(LAG(ca.active_customers) OVER (
            PARTITION BY ca.cohort_month 
            ORDER BY ca.activity_month
        ), 0), 2) as mom_retention_change_pct,
        
        
        CASE 
            WHEN ca.months_since_first_purchase = 0 THEN 'Acquisition Month'
            WHEN ca.months_since_first_purchase <= 3 THEN 'Early Stage (0-3 mo)'
            WHEN ca.months_since_first_purchase <= 6 THEN 'Growth Stage (4-6 mo)'
            WHEN ca.months_since_first_purchase <= 12 THEN 'Maturity Stage (7-12 mo)'
            ELSE 'Loyal Customers (12+ mo)'
        END as cohort_stage,
        
        
        CASE 
            WHEN ca.months_since_first_purchase = 0 THEN NULL
            WHEN retention_rate_pct >= 80 THEN 'Excellent Retention'
            WHEN retention_rate_pct >= 60 THEN 'Good Retention'
            WHEN retention_rate_pct >= 40 THEN 'Average Retention'
            WHEN retention_rate_pct >= 20 THEN 'Poor Retention'
            ELSE 'Critical Churn'
        END as retention_health,
        
        
        CASE 
            WHEN avg_orders_per_customer >= 3 THEN 'Highly Engaged'
            WHEN avg_orders_per_customer >= 2 THEN 'Engaged'
            WHEN avg_orders_per_customer >= 1 THEN 'Moderately Engaged'
            ELSE 'Low Engagement'
        END as engagement_level,
        
        CURRENT_TIMESTAMP as mart_created_at
        
    FROM cohort_activity ca
    INNER JOIN cohort_base_metrics cbm
        ON ca.cohort_month = cbm.cohort_month
    
    WHERE ca.activity_month IS NOT NULL  
)

SELECT * FROM final
ORDER BY cohort_month DESC, months_since_first_purchase ASC