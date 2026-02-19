{{
    config(
        materialized='incremental',
        unique_key='customer_key',
        on_schema_change='fail',
        tags=['gold', 'dimension', 'scd2']
    )
}}

{% if is_incremental() %}

WITH new_and_changed AS (
    SELECT 
        stg.*
    FROM {{ ref('stg_customers') }} stg
    LEFT JOIN {{ this }} dim
        ON stg.customer_id = dim.customer_id
        AND dim.is_current = TRUE
    WHERE 
        dim.customer_key IS NULL
        OR (
            stg.street_address IS DISTINCT FROM dim.street_address
            OR stg.city IS DISTINCT FROM dim.city
            OR stg.state IS DISTINCT FROM dim.state
            OR stg.zip_code IS DISTINCT FROM dim.zip_code
            OR stg.customer_segment IS DISTINCT FROM dim.customer_segment
            OR stg.ltv_category IS DISTINCT FROM dim.ltv_category
            OR stg.customer_tenure_category IS DISTINCT FROM dim.customer_tenure_category
        )
),

closed_records AS (
    SELECT 
        dim.customer_key,
        dim.customer_id,
        dim.first_name,
        dim.last_name,
        dim.full_name,
        dim.gender,
        dim.date_of_birth,
        dim.age,
        dim.age_group,
        dim.email,
        dim.phone,
        dim.phone_extension,
        dim.phone_digits_only,
        dim.street_address,
        dim.city,
        dim.state,
        dim.zip_code,
        dim.state_code_from_zip,
        dim.state_name_from_zip,
        dim.county_name,
        dim.fips_code,
        dim.latitude,
        dim.longitude,
        dim.registration_date,
        dim.days_since_registration,
        dim.customer_tenure_category,
        dim.customer_segment,
        dim.lifetime_value,
        dim.ltv_category,
        dim.quality_tier,
        dim.quality_issues_count,
        dim.valid_start_date,
        CURRENT_TIMESTAMP AS valid_end_date,
        FALSE AS is_current,
        dim.created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM {{ this }} dim
    INNER JOIN new_and_changed stg
        ON dim.customer_id = stg.customer_id
        AND dim.is_current = TRUE
),

new_versions AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'CURRENT_TIMESTAMP']) }} AS customer_key,
        customer_id,
        first_name,
        last_name,
        full_name,
        gender,
        date_of_birth,
        age,
        age_group,
        email,
        phone,
        phone_extension,
        phone_digits_only,
        street_address,
        city,
        state,
        zip_code,
        state_code_from_zip,
        state_name_from_zip,
        county_name,
        fips_code,
        latitude,
        longitude,
        registration_date,
        days_since_registration,
        customer_tenure_category,
        customer_segment,
        lifetime_value,
        ltv_category,
        quality_tier,
        quality_issues_count,
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
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'CURRENT_TIMESTAMP']) }} AS customer_key,
    customer_id,
    first_name,
    last_name,
    full_name,
    gender,
    date_of_birth,
    age,
    age_group,
    email,
    phone,
    phone_extension,
    phone_digits_only,
    street_address,
    city,
    state,
    zip_code,
    state_code_from_zip,
    state_name_from_zip,
    county_name,
    fips_code,
    latitude,
    longitude,
    registration_date,
    days_since_registration,
    customer_tenure_category,
    customer_segment,
    lifetime_value,
    ltv_category,
    quality_tier,
    quality_issues_count,
    CURRENT_TIMESTAMP AS valid_start_date,
    NULL AS valid_end_date,
    TRUE AS is_current,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('stg_customers') }}

{% endif %}
