{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        on_schema_change='fail',
        tags=['staging', 'incremental']
    )
}}

WITH source_data AS (
    SELECT * FROM {{ ref('raw_customers') }}
    
    {% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1 
        FROM {{ this }} existing
        WHERE existing.customer_id = {{ ref('raw_customers') }}.customer_id
          AND existing._row_hash = MD5(
              COALESCE({{ ref('raw_customers') }}.first_name, '') || '|' ||
              COALESCE({{ ref('raw_customers') }}.last_name, '') || '|' ||
              COALESCE({{ ref('raw_customers') }}.email, '') || '|' ||
              COALESCE({{ ref('raw_customers') }}.city, '') || '|' ||
              COALESCE({{ ref('raw_customers') }}.state, '') || '|' ||
              COALESCE({{ ref('raw_customers') }}.zip_code, '')
          )
    )
    {% endif %}
),

staging_cleaned AS (
    SELECT
        customer_id,

        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,

        TRIM(first_name) || ' ' || TRIM(last_name) AS full_name, 
        CASE 
            WHEN email IS NULL OR TRIM(email) = '' THEN NULL
            WHEN LOWER(TRIM(REGEXP_REPLACE(email, ' at ', '@', 'g'))) LIKE '%@%.%' THEN LOWER(TRIM(REGEXP_REPLACE(email, ' at ', '@', 'g')))
            WHEN LOWER(TRIM(REGEXP_REPLACE(email, ' at ', '@', 'g'))) NOT LIKE '%@%.%' THEN NULL
            ELSE NULL
        END AS email,

        REGEXP_REPLACE(phone, '[^0-9x+()-]', '', 'g') AS phone,
    
        phone AS phone_raw,
        
        CASE 
            WHEN phone IS NULL OR TRIM(phone) = '' THEN 1
            WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) < 7 THEN 1
            ELSE 0
        END AS invalid_phone_flag,

        CASE 
            WHEN address IS NULL OR TRIM(address) = '' THEN NULL
            WHEN POSITION(E'\n' IN address) > 0 THEN 
                TRIM(SPLIT_PART(address, E'\n', 1))
            WHEN POSITION(',' IN address) > 0 THEN 
                TRIM(SPLIT_PART(address, ',', 1))
            ELSE TRIM(address)
        END AS street_address,

        address AS address_raw,

        TRIM(city) AS city,
        TRIM(state) AS state,
        TRIM(zip_code) AS zip_code,

        CASE
            WHEN zip_code::VARCHAR IN 
            (SELECT zip_code FROM {{ ref('us_zip_fips_county')}} ) THEN 0
            ELSE 1
        END AS invalid_zip_code_flag,

        CASE
            WHEN (state,zip_code) IN(
                SELECT 
                a.state, a.zip_code
                FROM {{ ref('raw_customers')}} a 
                JOIN {{ ref('us_zip_fips_county')}} b 
                ON a.state=b.state_name
                AND a.zip_code=b.zip_code
            ) THEN 0
            ELSE 1
        END AS state_zip_mismatch_flag,

        CASE 
            WHEN address IS NULL THEN 0
            WHEN city IS NOT NULL AND address NOT LIKE '%' || city || '%' THEN 1
            ELSE 0
        END AS address_city_mismatch_flag,

        CASE 
            WHEN address IS NULL THEN 0
            WHEN state IS NOT NULL AND address NOT LIKE '%' || state || '%' THEN 1
            ELSE 0
        END AS address_state_mismatch_flag,

        CASE 
            WHEN address IS NULL THEN 0
            WHEN zip_code IS NOT NULL AND address NOT LIKE '%' || zip_code || '%' THEN 1
            ELSE 0
        END AS address_zip_mismatch_flag,

        CASE WHEN city IS NULL OR TRIM(city) = '' THEN 1 ELSE 0 END AS missing_city_flag,
        CASE WHEN state IS NULL OR TRIM(state) = '' THEN 1 ELSE 0 END AS missing_state_flag,
        CASE WHEN zip_code IS NULL OR TRIM(zip_code) = '' THEN 1 ELSE 0 END AS missing_zip_code_flag,

        CASE 
            WHEN date_of_birth IS NULL OR TRIM(date_of_birth::VARCHAR) = '' THEN NULL
            ELSE TRY_CAST(date_of_birth AS DATE)
        END AS date_of_birth,

        CASE 
            WHEN date_of_birth IS NULL OR TRIM(date_of_birth::VARCHAR) = '' THEN NULL
            ELSE DATE_DIFF('year', TRY_CAST(date_of_birth AS DATE), CURRENT_DATE)
        END AS age,

        CASE 
            WHEN DATE_DIFF('year', TRY_CAST(date_of_birth AS DATE), CURRENT_DATE)<18 THEN 'Under 18'
            WHEN DATE_DIFF('year', TRY_CAST(date_of_birth AS DATE), CURRENT_DATE) BETWEEN 18 AND 24 THEN '18-24'
            WHEN DATE_DIFF('year', TRY_CAST(date_of_birth AS DATE), CURRENT_DATE) BETWEEN 25 AND 34 THEN '25-34'
            WHEN DATE_DIFF('year', TRY_CAST(date_of_birth AS DATE), CURRENT_DATE) BETWEEN 35 AND 44 THEN '35-44'
            WHEN DATE_DIFF('year', TRY_CAST(date_of_birth AS DATE), CURRENT_DATE) BETWEEN 45 AND 54 THEN '45-54'
            WHEN DATE_DIFF('year', TRY_CAST(date_of_birth AS DATE), CURRENT_DATE) BETWEEN 55 AND 64 THEN '55-64'
            ELSE '65+'
        END AS age_group,

        CASE 
            WHEN UPPER(TRIM(gender)) IN ('MALE', 'M') THEN 'Male'
            WHEN UPPER(TRIM(gender)) IN ('FEMALE', 'F') THEN 'Female'
            WHEN UPPER(TRIM(gender)) = 'OTHER' THEN 'Other'
            ELSE NULL
        END AS gender,

        CASE 
            WHEN registration_date IS NULL OR TRIM(registration_date::VARCHAR) = '' THEN NULL
            ELSE TRY_CAST(registration_date AS DATE)
        END AS registration_date,

        CASE 
            WHEN registration_date IS NULL OR TRIM(registration_date::VARCHAR) = '' THEN NULL
            ELSE DATE_DIFF('day', TRY_CAST(registration_date AS DATE), CURRENT_DATE)
        END AS days_since_registration,

        CASE
            WHEN DATE_DIFF('month', TRY_CAST(registration_date AS DATE), CURRENT_DATE)>24 THEN 'Loyal (2+ years)'
            WHEN DATE_DIFF('month', TRY_CAST(registration_date AS DATE), CURRENT_DATE) BETWEEN 12 AND 24 THEN 'Established (1-2 years)'
            WHEN DATE_DIFF('month', TRY_CAST(registration_date AS DATE), CURRENT_DATE)>24 BETWEEN 3 AND 12 THEN 'Recent (3-12 months)'
            ELSE 'New (< 3 months)'
        END AS customer_tenure_category,

        COALESCE(loyalty_member, FALSE) AS loyalty_member,

        CASE 
            WHEN UPPER(TRIM(preferred_contact)) = 'EMAIL' THEN 'Email'
            WHEN UPPER(TRIM(preferred_contact)) = 'PHONE' THEN 'Phone'
            WHEN UPPER(TRIM(preferred_contact)) = 'SMS' THEN 'SMS'
            ELSE NULL
        END AS preferred_contact,

        CASE 
            WHEN UPPER(TRIM(customer_segment)) = 'VIP' THEN 'VIP'
            WHEN UPPER(TRIM(customer_segment)) = 'PREMIUM' THEN 'Premium'
            WHEN UPPER(TRIM(customer_segment)) = 'REGULAR' THEN 'Regular'
            WHEN UPPER(TRIM(customer_segment)) = 'BUDGET' THEN 'Budget'
            ELSE 'Unknown'
        END AS customer_segment,

        total_lifetime_value,
        CASE WHEN total_lifetime_value < 0 THEN 1 ELSE 0 END AS negative_ltv_flag,
        CASE 
            WHEN total_lifetime_value >= 5000 THEN 'High Value'
            WHEN total_lifetime_value >= 2000 THEN 'Medium Value'
            WHEN total_lifetime_value >= 500 THEN 'Low Value'
            WHEN total_lifetime_value < 0 THEN 'Negative'
            ELSE 'Very Low Value'
        END AS ltv_tier,

        CASE WHEN email IS NULL OR TRIM(email) = '' THEN 1 ELSE 0 END AS missing_email_flag,
        CASE WHEN phone IS NULL OR TRIM(phone) = '' THEN 1 ELSE 0 END AS missing_phone_flag,

        TRIM(REGEXP_REPLACE(first_name, '[^\x20-\x7E]', '', 'g')) AS first_name_clean,
        TRIM(REGEXP_REPLACE(last_name, '[^\x20-\x7E]', '', 'g')) AS last_name_clean,

        modified_date AS raw_loaded_at,
        CURRENT_TIMESTAMP AS staging_loaded_at,
        '{{ run_started_at }}' AS staging_batch_id,
        '{{ var("source_system", "RETAIL_S3") }}' AS _source_system,

        MD5(
            COALESCE(customer_id, '') || '|' ||
            COALESCE(first_name, '') || '|' ||
            COALESCE(last_name, '') || '|' ||
            COALESCE(email, '') || '|' ||
            COALESCE(phone, '') || '|' ||
            COALESCE(city, '') || '|' ||
            COALESCE(state, '') || '|' ||
            COALESCE(zip_code, '') || '|' ||
            COALESCE(CAST(total_lifetime_value AS VARCHAR), '') 
        ) AS _row_hash,

        'raw_customers' AS _source_table

    FROM source_data
)

SELECT * FROM staging_cleaned