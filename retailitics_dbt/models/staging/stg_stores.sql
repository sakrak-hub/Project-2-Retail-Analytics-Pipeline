{{
    config(
        materialized='incremental',
        unique_key='store_id',
        on_schema_change='fail',
        tags=['staging', 'silver', 'stores']
    )
}}

WITH bronze_source AS (
    SELECT * 
    FROM {{ ref('bronze_stores') }}
    
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(bronze_loaded_at) FROM {{ this }})
    {% endif %}
),

filtered AS (
    SELECT *
    FROM bronze_source

    WHERE
        store_id IS NOT NULL
        AND missing_store_name_flag = 0
        AND missing_city_flag = 0
        AND missing_state_flag = 0
        AND missing_zip_code_flag = 0
        AND missing_phone_flag = 0
        AND phone_too_short_flag = 0
        AND missing_opening_date_flag = 0
        AND future_opening_date_flag = 0
),

staging_final AS (
    SELECT
        store_id,
        store_name,
        store_type,
        manager,
        store_address,
        city,
        state,
        zip_code,
        store_address || ', ' || city || ', ' || state || ' ' || zip_code AS full_address,
        CASE 
            WHEN address_city_mismatch_flag = 1 
                OR address_state_mismatch_flag = 1 
                OR address_zip_mismatch_flag = 1 
            THEN 'INCONSISTENT'
            ELSE 'VALID'
        END AS address_quality,

        store_phone,
        opening_date,
        days_since_opening,
        years_since_opening,
        store_age_category,

        CASE 
            WHEN years_since_opening IS NULL THEN 'Unknown'
            WHEN years_since_opening < 1 THEN 'Brand New'
            WHEN years_since_opening < 3 THEN 'Growing'
            WHEN years_since_opening < 5 THEN 'Established'
            WHEN years_since_opening < 10 THEN 'Mature'
            ELSE 'Legacy'
        END AS store_maturity_tier,

        is_fully_operational,
        
        CASE 
            WHEN very_old_store_flag = 1 THEN TRUE
            ELSE FALSE
        END AS is_legacy_store,

        CASE 
            WHEN store_type IN ('FLAGSHIP', 'MAIN') THEN 'Flagship'
            WHEN store_type IN ('OUTLET', 'CLEARANCE') THEN 'Outlet'
            WHEN store_type IN ('KIOSK', 'POP-UP') THEN 'Temporary'
            WHEN store_type = 'WAREHOUSE' THEN 'Warehouse'
            ELSE 'Standard'
        END AS store_category,

        CASE 
            WHEN state IN ('CA', 'OR', 'WA', 'NV', 'AZ') THEN 'West'
            WHEN state IN ('TX', 'OK', 'AR', 'LA', 'MS', 'AL', 'TN', 'KY') THEN 'South'
            WHEN state IN ('NY', 'NJ', 'PA', 'CT', 'MA', 'VT', 'NH', 'ME', 'RI') THEN 'Northeast'
            WHEN state IN ('IL', 'IN', 'MI', 'OH', 'WI', 'MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS') THEN 'Midwest'
            ELSE 'Other'
        END AS region,

        CASE 
            WHEN city IN ('New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 
                          'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose',
                          'Austin', 'Jacksonville', 'Fort Worth', 'Columbus', 'Charlotte',
                          'San Francisco', 'Indianapolis', 'Seattle', 'Denver', 'Boston') 
            THEN TRUE
            ELSE FALSE
        END AS is_metro_area,

        CASE 
            WHEN address_city_mismatch_flag = 0 
                AND address_state_mismatch_flag = 0 
                AND address_zip_mismatch_flag = 0 
                AND missing_manager_flag = 0
            THEN 'COMPLETE'
            WHEN address_city_mismatch_flag = 0 
                AND address_state_mismatch_flag = 0 
                AND address_zip_mismatch_flag = 0
            THEN 'GOOD'
            ELSE 'BASIC'
        END AS store_data_quality,

        _loaded_at AS bronze_loaded_at,
        CURRENT_TIMESTAMP AS staging_loaded_at,
        _batch_id AS bronze_batch_id,
        '{{ invocation_id }}' AS staging_batch_id,
        _source_system
    
    FROM filtered
)

SELECT * FROM staging_final