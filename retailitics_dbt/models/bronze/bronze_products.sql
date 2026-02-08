{{
    config(
        materialized='incremental',
        unique_key='product_id',
        on_schema_change='fail',
        tags=['bronze', 'incremental']
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

bronze_cleaned AS (
    SELECT
        product_id,
        TRIM(REGEXP_REPLACE(product_name, '[^\x20-\x7E]', '', 'g')) AS product_name,
        product_name AS product_name_raw,
        TRIM(REGEXP_REPLACE(category, '[^\x20-\x7E]', '', 'g')) AS category,
        TRIM(REGEXP_REPLACE(subcategory, '[^\x20-\x7E]', '', 'g')) AS subcategory,
        TRIM(REGEXP_REPLACE(brand, '[^\x20-\x7E]', '', 'g')) AS brand,
        TRIM(sku) AS sku,
        ROUND(price,2) AS selling_price,
        price AS selling_price_raw,
        ROUND(cost,2) AS cost_price,
        cost AS cost_price_raw,
        weight,
        dimensions,
        
        CASE
            WHEN stock_quantity<0 THEN NULL
            ELSE stock_quantity
            END as stock_quantity,
        stock_quantity AS stock_quantity_raw,

        TRIM(REGEXP_REPLACE(supplier, '[^\x20-\x7E]', '', 'g')) AS supplier,

        CASE 
            WHEN launch_date IS NULL OR TRIM(launch_date::VARCHAR)='' THEN NULL
            ELSE TRY_CAST(launch_date AS DATE)
            END as launch_date,
        
        CASE WHEN product_name IS NULL OR TRIM(product_name) = '' THEN 1 ELSE 0 END AS missing_product_name_flag,
        CASE WHEN category IS NULL OR TRIM(category) = '' THEN 1 ELSE 0 END AS missing_category_flag,
        CASE WHEN sku IS NULL OR TRIM(sku) = '' THEN 1 ELSE 0 END AS missing_sku_flag,
        CASE WHEN supplier IS NULL OR TRIM(supplier) = '' THEN 1 ELSE 0 END AS missing_supplier_flag,

        CASE WHEN stock_quantity < 0 THEN 1 ELSE 0 END AS negative_stock_quantity_flag,
        CASE WHEN stock_quantity IS NULL THEN 1 ELSE 0 END AS missing_stock_quantity_flag,
        CASE WHEN stock_quantity = 0 THEN 1 ELSE 0 END AS zero_stock_quantity_flag,

        CASE WHEN price IS NULL OR price <= 0 THEN 1 ELSE 0 END AS invalid_price_flag,
        CASE WHEN cost IS NULL OR cost < 0 THEN 1 ELSE 0 END AS invalid_cost_flag,
        CASE WHEN cost > price THEN 1 ELSE 0 END AS cost_exceeds_price_flag,

        CASE WHEN price > 10000 THEN 1 ELSE 0 END AS very_high_price_flag,
        CASE WHEN price < 1.0 AND price > 0 THEN 1 ELSE 0 END AS very_low_price_flag,

        CASE WHEN weight IS NULL THEN 1 ELSE 0 END AS missing_weight_flag,
        CASE WHEN weight < 0.01 AND weight > 0 THEN 1 ELSE 0 END AS suspiciously_low_weight_flag,
        CASE WHEN weight > 1000 THEN 1 ELSE 0 END AS very_high_weight_flag,

        CASE WHEN dimensions IS NULL OR TRIM(dimensions::VARCHAR) = '' THEN 1 ELSE 0 END AS missing_dimensions_flag,

        CASE WHEN launch_date IS NULL THEN 1 ELSE 0 END AS missing_launch_date_flag,
        CASE 
            WHEN TRY_CAST(launch_date AS DATE) > CURRENT_DATE THEN 1 
            ELSE 0 
        END AS future_launch_date_flag,

        CASE 
            WHEN price > 0 AND cost >= 0 THEN 
                ROUND(((price - cost) / price) * 100, 2)
            ELSE NULL
        END AS profit_margin_pct,

        CASE 
            WHEN price >= 1000 THEN 'Premium'
            WHEN price >= 100 THEN 'Mid-Range'
            WHEN price >= 10 THEN 'Budget'
            WHEN price > 0 THEN 'Economy'
            ELSE 'Invalid'
        END AS price_tier,

        CASE 
            WHEN stock_quantity IS NULL THEN 'Unknown'
            WHEN stock_quantity = 0 THEN 'Out of Stock'
            WHEN stock_quantity < 10 THEN 'Low Stock'
            WHEN stock_quantity < 50 THEN 'Available'
            ELSE 'In Stock'
        END AS stock_status,

        CASE 
            WHEN TRY_CAST(launch_date AS DATE) IS NOT NULL 
                AND TRY_CAST(launch_date AS DATE) <= CURRENT_DATE 
            THEN DATE_DIFF('day', TRY_CAST(launch_date AS DATE), CURRENT_DATE)
            ELSE NULL
        END AS days_since_launch,

        CURRENT_TIMESTAMP AS _loaded_at,
        '{{ run_started_at }}' AS _batch_id,
        '{{ var("source_system", "RETAIL_S3") }}' AS _source_system,

        MD5(
            COALESCE(product_name, '') || '|' ||
            COALESCE(category, '') || '|' ||
            COALESCE(subcategory, '') || '|' ||
            COALESCE(brand, '') || '|' ||
            COALESCE(CAST(price AS VARCHAR), '') || '|' ||
            COALESCE(CAST(cost AS VARCHAR), '') || '|' ||
            COALESCE(sku, '') || '|' ||
            COALESCE(CAST(stock_quantity AS VARCHAR), '') || '|' ||
            COALESCE(supplier, '') || '|' ||
            COALESCE(CAST(launch_date AS VARCHAR), '')
        ) AS _row_hash,

        'raw_products' AS _source_table

    FROM source_data
)

SELECT * FROM bronze_cleaned;