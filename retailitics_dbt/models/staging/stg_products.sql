{{
    config(
        materialized='incremental',
        unique_key='product_id',
        on_schema_change='fail',
        tags=['staging', 'silver', 'products']
    )
}}

WITH bronze_source AS (
    SELECT * 
    FROM {{ ref('bronze_products') }}
    
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(bronze_loaded_at) FROM {{ this }})
    {% endif %}
),

filtered AS (
    SELECT *
    FROM bronze_source

    WHERE
        product_id IS NOT NULL
        AND missing_product_name_flag = 0
        AND invalid_price_flag = 0
        AND invalid_cost_flag = 0
        AND cost_exceeds_price_flag = 0
        AND missing_category_flag = 0
),

staging_final AS (
    SELECT
        product_id,
        product_name,
        category,
        subcategory,
        brand,
        sku,
        supplier,
        selling_price,
        cost_price,
        profit_margin_pct,
        price_tier,
        CASE 
            WHEN very_high_price_flag = 1 THEN 'LUXURY'
            WHEN very_low_price_flag = 1 THEN 'CLEARANCE'
            ELSE 'STANDARD'
        END AS price_category,
        stock_quantity,
        stock_status,
        CASE 
            WHEN stock_quantity > 0 THEN TRUE
            ELSE FALSE
        END AS is_available,
        
        CASE 
            WHEN stock_quantity = 0 THEN TRUE
            ELSE FALSE
        END AS is_out_of_stock,
        
        CASE 
            WHEN stock_quantity > 0 AND stock_quantity < 10 THEN TRUE
            ELSE FALSE
        END AS is_low_stock,

        weight,
        dimensions,
        CASE 
            WHEN weight IS NULL THEN 'Unknown'
            WHEN weight < 1 THEN 'Light (< 1 kg)'
            WHEN weight < 5 THEN 'Medium (1-5 kg)'
            WHEN weight < 25 THEN 'Heavy (5-25 kg)'
            ELSE 'Very Heavy (25+ kg)'
        END AS weight_category,
        launch_date,
        days_since_launch,

        CASE 
            WHEN days_since_launch IS NULL THEN 'Unknown'
            WHEN days_since_launch < 30 THEN 'New (< 1 month)'
            WHEN days_since_launch < 90 THEN 'Recent (1-3 months)'
            WHEN days_since_launch < 365 THEN 'Current (< 1 year)'
            WHEN days_since_launch < 730 THEN 'Established (1-2 years)'
            ELSE 'Mature (2+ years)'
        END AS product_maturity,
        
        CASE 
            WHEN future_launch_date_flag = 1 THEN TRUE
            ELSE FALSE
        END AS is_upcoming,

        CASE 
            WHEN profit_margin_pct IS NULL THEN 'Unknown'
            WHEN profit_margin_pct < 10 THEN 'Low Margin (< 10%)'
            WHEN profit_margin_pct < 25 THEN 'Moderate Margin (10-25%)'
            WHEN profit_margin_pct < 50 THEN 'Good Margin (25-50%)'
            ELSE 'High Margin (50%+)'
        END AS margin_tier,

        CASE 
            WHEN missing_sku_flag = 0 
                AND missing_supplier_flag = 0 
                AND missing_weight_flag = 0 
                AND missing_dimensions_flag = 0 
                AND missing_launch_date_flag = 0
            THEN 'COMPLETE'
            WHEN missing_sku_flag = 0 
                AND missing_supplier_flag = 0
            THEN 'GOOD'
            ELSE 'BASIC'
        END AS product_data_quality,

        _loaded_at AS bronze_loaded_at,
        CURRENT_TIMESTAMP AS staging_loaded_at,
        _batch_id AS bronze_batch_id,
        '{{ invocation_id }}' AS staging_batch_id,
        _source_system
        
    FROM filtered
)

SELECT * FROM staging_final