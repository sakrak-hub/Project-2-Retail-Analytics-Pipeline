{{
    config(
        materialized='incremental',
        unique_key='product_id',
        on_schema_change='fail',
        tags=['staging', 'incremental']
    )
}}

WITH source_data AS (
    SELECT * FROM {{ ref('raw_products') }}
    
    {% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1 
        FROM {{ this }} existing
        WHERE existing.product_id = {{ ref('raw_products') }}.product_id
          AND existing._row_hash = MD5(
              COALESCE({{ ref('raw_products') }}.product_name, '') || '|' ||
              COALESCE({{ ref('raw_products') }}.category, '') || '|' ||
              COALESCE({{ ref('raw_products') }}.subcategory, '') || '|' ||
              COALESCE(CAST({{ ref('raw_products') }}.price AS VARCHAR), '') || '|' ||
              COALESCE(CAST({{ ref('raw_products') }}.cost AS VARCHAR), '') || '|' ||
              COALESCE(CAST({{ ref('raw_products') }}.stock_quantity AS VARCHAR), '')
          )
    )
    {% endif %}
),

cleaned_product_name AS(
    WITH product_names AS(
    SELECT
    DISTINCT(TRIM(REGEXP_REPLACE(product_name, '[^\x20-\x7E]', '', 'g'))) AS cleaned_product_name,
    product_name as raw_product_name,
    product_id
    FROM raw_db.raw_products
    ORDER BY 1
    )
    SELECT 
    product_id,
    cleaned_product_name
    FROM product_names 
    WHERE cleaned_product_name = raw_product_name
),

staging_cleaned AS (
    SELECT
        sd.product_id,
        pn.cleaned_product_name AS product_name,
        sd.product_name AS product_name_raw,
        TRIM(REGEXP_REPLACE(sd.category, '[^\x20-\x7E]', '', 'g')) AS category,
        TRIM(REGEXP_REPLACE(sd.subcategory, '[^\x20-\x7E]', '', 'g')) AS subcategory,
        TRIM(REGEXP_REPLACE(sd.brand, '[^\x20-\x7E]', '', 'g')) AS brand,
        TRIM(sd.sku) AS sku,
        ROUND(sd.price,2) AS selling_price,
        sd.price AS selling_price_raw,
        ROUND(sd.cost,2) AS cost_price,
        sd.cost AS cost_price_raw,
        sd.weight,
        sd.dimensions,
        
        CASE
            WHEN sd.stock_quantity<0 THEN NULL
            ELSE sd.stock_quantity
            END as stock_quantity,
        sd.stock_quantity AS stock_quantity_raw,

        TRIM(REGEXP_REPLACE(sd.supplier, '[^\x20-\x7E]', '', 'g')) AS supplier,

        CASE 
            WHEN sd.launch_date IS NULL OR TRIM(sd.launch_date::VARCHAR)='' THEN NULL
            ELSE TRY_CAST(sd.launch_date AS DATE)
        END as launch_date,

        CASE
            WHEN (sd.launch_date::DATE)>today() THEN 1
            ELSE 0
        END AS is_upcoming,

        CASE
            WHEN DATE_DIFF('month', sd.launch_date::DATE, today())<1 THEN 'New (< 1 month)'
            WHEN DATE_DIFF('month', sd.launch_date::DATE, today()) BETWEEN 1 AND 3 THEN 'Recent (1-3 months)'
            WHEN DATE_DIFF('month', sd.launch_date::DATE, today()) BETWEEN 3 AND 12 THEN 'Current (< 1 year)'
            WHEN DATE_DIFF('month', sd.launch_date::DATE, today()) BETWEEN 12 AND 24 THEN 'Established (1-2 years)'
            WHEN DATE_DIFF('month', sd.launch_date::DATE, today())>24 THEN 'Mature (2+ years)'
            ELSE 'Unknown'
        END AS product_maturity,
        
        CASE WHEN sd.product_name IS NULL OR TRIM(sd.product_name) = '' THEN 1 ELSE 0 END AS missing_product_name_flag,
        CASE WHEN sd.category IS NULL OR TRIM(sd.category) = '' THEN 1 ELSE 0 END AS missing_category_flag,
        CASE WHEN sd.sku IS NULL OR TRIM(sd.sku) = '' THEN 1 ELSE 0 END AS missing_sku_flag,
        CASE WHEN sd.supplier IS NULL OR TRIM(sd.supplier) = '' THEN 1 ELSE 0 END AS missing_supplier_flag,

        CASE WHEN sd.stock_quantity < 0 THEN 1 ELSE 0 END AS negative_stock_quantity_flag,
        CASE WHEN sd.stock_quantity IS NULL THEN 1 ELSE 0 END AS missing_stock_quantity_flag,
        CASE WHEN sd.stock_quantity = 0 THEN 1 ELSE 0 END AS zero_stock_quantity_flag,
        CASE WHEN sd.stock_quantity BETWEEN 1 AND 9 THEN 1 ELSE 0 END AS is_low_stock,

        CASE WHEN sd.price IS NULL OR sd.price <= 0 THEN 1 ELSE 0 END AS invalid_price_flag,
        CASE WHEN sd.cost IS NULL OR sd.cost < 0 THEN 1 ELSE 0 END AS invalid_cost_flag,
        CASE WHEN sd.cost > sd.price THEN 1 ELSE 0 END AS cost_exceeds_price_flag,

        CASE WHEN sd.price > 10000 THEN 1 ELSE 0 END AS very_high_price_flag,
        CASE WHEN sd.price < 1.0 AND sd.price > 0 THEN 1 ELSE 0 END AS very_low_price_flag,

        CASE WHEN sd.weight IS NULL THEN 1 ELSE 0 END AS missing_weight_flag,
        CASE WHEN sd.weight < 0.01 AND sd.weight > 0 THEN 1 ELSE 0 END AS suspiciously_low_weight_flag,
        CASE WHEN sd.weight > 1000 THEN 1 ELSE 0 END AS very_high_weight_flag,

        CASE 
            WHEN sd.weight<1 THEN 'Light (< 1 kg)'
            WHEN sd.weight BETWEEN 1 AND 5 THEN 'Medium (1-5 kg)'
            WHEN sd.weight BETWEEN 5 AND 25 THEN 'Heavy (5-25 kg)'
            WHEN sd.weight>25 THEN 'Very Heavy (25+ kg)'
            ELSE 'Unknown'
        END AS weight_category,

        CASE WHEN sd.dimensions IS NULL OR TRIM(sd.dimensions::VARCHAR) = '' THEN 1 ELSE 0 END AS missing_dimensions_flag,

        CASE WHEN sd.launch_date IS NULL THEN 1 ELSE 0 END AS missing_launch_date_flag,
        CASE 
            WHEN TRY_CAST(sd.launch_date AS DATE) > CURRENT_DATE THEN 1 
            ELSE 0 
        END AS future_launch_date_flag,

        CASE 
            WHEN sd.price > 0 AND sd.cost >= 0 THEN 
                ROUND(((sd.price - cost) / sd.price) * 100, 2)
            ELSE NULL
        END AS profit_margin_pct,

        CASE
            WHEN sd.price > 0 AND cost >= 0 AND ROUND(((sd.price - sd.cost) / price) * 100, 2)<10 THEN 'Low Margin (< 10%)'
            WHEN sd.price > 0 AND cost >= 0 AND ROUND(((sd.price - sd.cost) / price) * 100, 2) BETWEEN 10 AND 25 THEN 'Moderate Margin (10-25%)'
            WHEN sd.price > 0 AND cost >= 0 AND ROUND(((sd.price - sd.cost) / price) * 100, 2) BETWEEN 25 AND 50 THEN 'Good Margin (25-50%)'
            WHEN sd.price > 0 AND cost >= 0 AND ROUND(((sd.price - sd.cost) / price) * 100, 2)>50 THEN 'High Margin (50%+)'
            ELSE 'Unknown'
        END AS margin_tier,

        CASE 
            WHEN sd.price >= 1000 THEN 'Premium'
            WHEN sd.price >= 100 THEN 'Mid-Range'
            WHEN sd.price >= 10 THEN 'Budget'
            WHEN sd.price > 0 THEN 'Economy'
            ELSE 'Invalid'
        END AS price_tier,

        CASE 
            WHEN sd.stock_quantity IS NULL THEN 'Unknown'
            WHEN sd.stock_quantity = 0 THEN 'Out of Stock'
            WHEN sd.stock_quantity < 10 THEN 'Low Stock'
            WHEN sd.stock_quantity < 50 THEN 'Available'
            ELSE 'In Stock'
        END AS stock_status,

        CASE 
            WHEN TRY_CAST(sd.launch_date AS DATE) IS NOT NULL 
                AND TRY_CAST(sd.launch_date AS DATE) <= CURRENT_DATE 
            THEN DATE_DIFF('day', TRY_CAST(sd.launch_date AS DATE), CURRENT_DATE)
            ELSE NULL
        END AS days_since_launch,

        sd.modified_date AS raw_loaded_at,
        CURRENT_TIMESTAMP AS staging_loaded_at,
        '{{ run_started_at }}' AS staging_batch_id,
        '{{ var("source_system", "RETAIL_S3") }}' AS _source_system,

        MD5(
            COALESCE(pn.cleaned_product_name, '') || '|' ||
            COALESCE(sd.category, '') || '|' ||
            COALESCE(sd.subcategory, '') || '|' ||
            COALESCE(sd.brand, '') || '|' ||
            COALESCE(CAST(sd.price AS VARCHAR), '') || '|' ||
            COALESCE(CAST(sd.cost AS VARCHAR), '') || '|' ||
            COALESCE(sd.sku, '') || '|' ||
            COALESCE(CAST(sd.stock_quantity AS VARCHAR), '') || '|' ||
            COALESCE(sd.supplier, '') || '|' ||
            COALESCE(CAST(sd.launch_date AS VARCHAR), '')
        ) AS _row_hash,

        'raw_products' AS _source_table

    FROM source_data sd
    LEFT JOIN cleaned_product_name pn 
    ON pn.product_id = sd.product_id
)

SELECT * FROM staging_cleaned