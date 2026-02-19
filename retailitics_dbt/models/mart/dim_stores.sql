{{
    config(
        materialized='incremental',
        unique_key='store_key',
        on_schema_change='fail',
        tags=['gold', 'dimension', 'scd2']
    )
}}

{% if is_incremental() %}

WITH new_and_changed AS (
    SELECT 
        stg.*
    FROM {{ ref('stg_stores') }} stg
    LEFT JOIN {{ this }} dim
        ON stg.store_id = dim.store_id
        AND dim.is_current = TRUE
    WHERE 
        dim.store_key IS NULL
        OR (
            stg.manager IS DISTINCT FROM dim.manager
            OR stg.store_type IS DISTINCT FROM dim.store_type
            OR stg.store_category IS DISTINCT FROM dim.store_category
            OR stg.is_fully_operational IS DISTINCT FROM dim.is_fully_operational
            OR stg.region IS DISTINCT FROM dim.region
        )
),

closed_records AS (
    SELECT 
        dim.store_key,
        dim.store_id,
        dim.store_name,
        dim.store_type,
        dim.store_category,
        dim.manager,
        dim.store_address,
        dim.full_address,
        dim.city,
        dim.state,
        dim.zip_code,
        dim.region,
        dim.is_metro_area,
        dim.store_phone,
        dim.opening_date,
        dim.days_since_opening,
        dim.years_since_opening,
        dim.store_age_category,
        dim.store_maturity_tier,
        dim.is_fully_operational,
        dim.is_legacy_store,
        dim.address_quality,
        dim.store_data_quality,
        dim.valid_start_date,
        CURRENT_TIMESTAMP AS valid_end_date,
        FALSE AS is_current,
        dim.created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM {{ this }} dim
    INNER JOIN new_and_changed stg
        ON dim.store_id = stg.store_id
        AND dim.is_current = TRUE
),

new_versions AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['store_id', 'CURRENT_TIMESTAMP']) }} AS store_key,
        store_id,
        store_name,
        store_type,
        store_category,
        manager,
        store_address,
        full_address,
        city,
        state,
        zip_code,
        region,
        is_metro_area,
        store_phone,
        opening_date,
        days_since_opening,
        years_since_opening,
        store_age_category,
        store_maturity_tier,
        is_fully_operational,
        is_legacy_store,
        address_quality,
        store_data_quality,
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
    {{ dbt_utils.generate_surrogate_key(['store_id', 'CURRENT_TIMESTAMP']) }} AS store_key,
    store_id,
    store_name,
    store_type,
    store_category,
    manager,
    store_address,
    full_address,
    city,
    state,
    zip_code,
    region,
    is_metro_area,
    store_phone,
    opening_date,
    days_since_opening,
    years_since_opening,
    store_age_category,
    store_maturity_tier,
    is_fully_operational,
    is_legacy_store,
    address_quality,
    store_data_quality,
    CURRENT_TIMESTAMP AS valid_start_date,
    NULL AS valid_end_date,
    TRUE AS is_current,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('stg_stores') }}

{% endif %}
