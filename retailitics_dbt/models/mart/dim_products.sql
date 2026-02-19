{{
    config(
        materialized='incremental',
        unique_key='product_key',
        on_schema_change='fail',
        tags=['gold', 'dimension', 'scd2']
    )
}}

{% if is_incremental() %}

WITH new_and_changed AS (
    SELECT 
        stg.*
    FROM {{ ref('stg_products') }} stg
    LEFT JOIN {{ this }} dim
        ON stg.product_id = dim.product_id
        AND dim.is_current = TRUE
    WHERE 
        dim.product_key IS NULL
        
        OR (
            stg.selling_price IS DISTINCT FROM dim.selling_price
            OR stg.cost_price IS DISTINCT FROM dim.cost_price
            OR stg.category IS DISTINCT FROM dim.category
            OR stg.subcategory IS DISTINCT FROM dim.subcategory
            OR stg.supplier IS DISTINCT FROM dim.supplier
            OR stg.price_tier IS DISTINCT FROM dim.price_tier
            OR stg.price_category IS DISTINCT FROM dim.price_category
        )
),

closed_records AS (
    SELECT 
        dim.product_key,
        dim.product_id,
        dim.product_name,
        dim.category,
        dim.subcategory,
        dim.brand,
        dim.sku,
        dim.supplier,
        dim.selling_price,
        dim.cost_price,
        dim.profit_margin_pct,
        dim.price_tier,
        dim.price_category,
        dim.stock_quantity,
        dim.stock_status,
        dim.is_available,
        dim.is_out_of_stock,
        dim.is_low_stock,
        dim.weight,
        dim.weight_category,
        dim.dimensions,
        dim.launch_date,
        dim.days_since_launch,
        dim.product_maturity,
        dim.is_upcoming,
        dim.margin_tier,
        dim.product_data_quality,
        dim.valid_start_date,
        CURRENT_TIMESTAMP AS valid_end_date,
        FALSE AS is_current,
        dim.created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM {{ this }} dim
    INNER JOIN new_and_changed stg
        ON dim.product_id = stg.product_id
        AND dim.is_current = TRUE
),

new_versions AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['product_id', 'CURRENT_TIMESTAMP']) }} AS product_key,
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
        price_category,
        stock_quantity,
        stock_status,
        is_available,
        is_out_of_stock,
        is_low_stock,
        weight,
        weight_category,
        dimensions,
        launch_date,
        days_since_launch,
        product_maturity,
        is_upcoming,
        margin_tier,
        product_data_quality,
        CURRENT_TIMESTAMP AS valid_start_date,
        NULL AS valid_end_date,
        TRUE AS is_current,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM new_and_changed
),

final AS (
    SELECT * FROM closed_records
    UNION ALL
    SELECT * FROM new_versions
)

SELECT * FROM final

{% else %}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['product_id', 'CURRENT_TIMESTAMP']) }} AS product_key,
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
    price_category,
    stock_quantity,
    stock_status,
    is_available,
    is_out_of_stock,
    is_low_stock,
    weight,
    weight_category,
    dimensions,
    launch_date,
    days_since_launch,
    product_maturity,
    is_upcoming,
    margin_tier,
    product_data_quality,
    CURRENT_TIMESTAMP AS valid_start_date,
    NULL AS valid_end_date,
    TRUE AS is_current,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('stg_products') }}

{% endif %}