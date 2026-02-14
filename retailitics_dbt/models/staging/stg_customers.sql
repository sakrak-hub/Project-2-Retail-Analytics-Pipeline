{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        on_schema_change='fail',
        tags=['silver', 'staging']
    )
}}

WITH bronze AS (
    SELECT * FROM {{ ref('bronze_customers') }}
    
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),

customers_with_zip_lookup AS (
    SELECT 
        bc.*,
        
        zc.state_abrv AS state_code_from_zip,
        zc.state_name AS state_name_from_zip,
        zc.county_name,
        zc.fips_code,
        zc.cent_lat AS latitude,
        zc.cent_long AS longitude
        
    FROM bronze bc
    LEFT JOIN {{ ref('us_zip_fips_county') }} zc
        ON bc.zip_code = zc.zip_code
),

staging_cleaned AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        full_name,

        gender,
        
        date_of_birth,
        age,

        CASE 
            WHEN age < 18 THEN 'Under 18'
            WHEN age < 25 THEN '18-24'
            WHEN age < 35 THEN '25-34'
            WHEN age < 45 THEN '35-44'
            WHEN age < 55 THEN '45-54'
            WHEN age < 65 THEN '55-64'
            ELSE '65+'
        END AS age_group,

        email,
        phone,
        CASE 
            WHEN phone LIKE '%x%' THEN REGEXP_EXTRACT(phone, 'x(\d+)', 1)
            ELSE NULL
        END AS phone_extension,
        REGEXP_REPLACE(
            REGEXP_REPLACE(phone, 'x.*$', ''),
            '[^0-9]', 
            ''
        ) AS phone_digits_only,

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

        CASE 
            WHEN days_since_registration < 90 THEN 'New (< 3 months)'
            WHEN days_since_registration < 365 THEN 'Recent (3-12 months)'
            WHEN days_since_registration < 730 THEN 'Established (1-2 years)'
            ELSE 'Loyal (2+ years)'
        END AS customer_tenure_category,
        
        customer_segment,
        lifetime_value,

        CASE 
            WHEN lifetime_value >= 5000 THEN 'High Value'
            WHEN lifetime_value >= 2000 THEN 'Medium Value'
            WHEN lifetime_value >= 500 THEN 'Low Value'
            WHEN lifetime_value >= 0 THEN 'Very Low Value'
            ELSE 'Negative'
        END AS ltv_category,

        CASE 
            WHEN zip_code IS NOT NULL 
                AND state_code_from_zip IS NULL 
            THEN 1 
            ELSE 0 
        END AS invalid_zip_code_flag,
        
        CASE 
            WHEN state IS NOT NULL 
                AND state_code_from_zip IS NOT NULL
                AND state != state_code_from_zip 
            THEN 1 
            ELSE 0 
        END AS state_zip_mismatch_flag,

        missing_email_flag,
        missing_phone_flag,
        missing_city_flag,
        missing_state_flag,
        missing_zip_code_flag,
        invalid_email_flag,
        negative_ltv_flag,

        address_city_mismatch_flag,
        address_state_mismatch_flag,
        address_zip_mismatch_flag,

        CASE 
            WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) < 10 
            THEN 1 
            ELSE 0 
        END AS phone_too_short_flag,

        (
            missing_email_flag +
            missing_phone_flag +
            missing_city_flag +
            missing_state_flag +
            missing_zip_code_flag +
            invalid_email_flag +
            negative_ltv_flag +
            address_city_mismatch_flag +
            address_state_mismatch_flag +
            address_zip_mismatch_flag +
            CASE WHEN zip_code IS NOT NULL AND state_code_from_zip IS NULL THEN 1 ELSE 0 END +
            CASE WHEN state IS NOT NULL AND state_code_from_zip IS NOT NULL AND state != state_code_from_zip THEN 1 ELSE 0 END +
            CASE WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) < 10 THEN 1 ELSE 0 END
        ) AS quality_issues_count,

        CASE 
            WHEN (
                missing_email_flag +
                missing_phone_flag +
                missing_city_flag +
                missing_state_flag +
                missing_zip_code_flag +
                invalid_email_flag +
                negative_ltv_flag +
                address_city_mismatch_flag +
                address_state_mismatch_flag +
                address_zip_mismatch_flag +
                CASE WHEN zip_code IS NOT NULL AND state_code_from_zip IS NULL THEN 1 ELSE 0 END +
                CASE WHEN state IS NOT NULL AND state_code_from_zip IS NOT NULL AND state != state_code_from_zip THEN 1 ELSE 0 END +
                CASE WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) < 10 THEN 1 ELSE 0 END
            ) = 0 THEN 'CLEAN'
            WHEN (
                missing_email_flag +
                missing_phone_flag +
                missing_city_flag +
                missing_state_flag +
                missing_zip_code_flag +
                invalid_email_flag +
                negative_ltv_flag +
                address_city_mismatch_flag +
                address_state_mismatch_flag +
                address_zip_mismatch_flag +
                CASE WHEN zip_code IS NOT NULL AND state_code_from_zip IS NULL THEN 1 ELSE 0 END +
                CASE WHEN state IS NOT NULL AND state_code_from_zip IS NOT NULL AND state != state_code_from_zip THEN 1 ELSE 0 END +
                CASE WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) < 10 THEN 1 ELSE 0 END
            ) <= 2 THEN 'ACCEPTABLE'
            ELSE 'POOR'
        END AS quality_tier,

        _loaded_at,
        _batch_id,
        _source_system,
        CURRENT_TIMESTAMP AS _staging_loaded_at
        
    FROM customers_with_zip_lookup

    WHERE missing_email_flag = 0           
      AND missing_city_flag = 0            
      AND missing_state_flag = 0           
      AND missing_zip_code_flag = 0        
      AND invalid_email_flag = 0           
      AND (
          missing_email_flag +
          missing_phone_flag +
          missing_city_flag +
          missing_state_flag +
          missing_zip_code_flag +
          invalid_email_flag +
          negative_ltv_flag
      ) <= 2 
)

SELECT * FROM staging_cleaned