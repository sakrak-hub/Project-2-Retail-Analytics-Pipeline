{{
    config(
        materialized='incremental',
        unique_key='store_id',
        on_schema_change='fail',
        tags=['bronze', 'incremental']
    )
}}

WITH source_data AS (
    SELECT * FROM {{ ref('raw_stores') }}
    
    {% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1 
        FROM {{ this }} existing
        WHERE existing.store_id = {{ ref('raw_stores') }}.store_id
          AND existing._row_hash = MD5(
              COALESCE({{ ref('raw_stores') }}.store_name, '') || '|' ||
              COALESCE({{ ref('raw_stores') }}.address, '') || '|' ||
              COALESCE({{ ref('raw_stores') }}.city, '') || '|' ||
              COALESCE({{ ref('raw_stores') }}.state, '') || '|' ||
              COALESCE(CAST({{ ref('raw_stores') }}.zip_code AS VARCHAR), '') || '|' ||
              COALESCE({{ ref('raw_stores') }}.phone, '') || '|' ||
              COALESCE({{ ref('raw_stores') }}.manager, '') || '|' ||
              COALESCE({{ ref('raw_stores') }}.store_type, '')
          )
    )
    {% endif %}
),

bronze_cleaned AS (
    SELECT
        store_id,
        TRIM(REGEXP_REPLACE(store_name, '[^\x20-\x7E]', '', 'g')) AS store_name,
        store_name AS store_name_raw,

        CASE 
                WHEN address IS NULL OR TRIM(address) = '' THEN NULL
                WHEN POSITION(E'\n' IN address) > 0 THEN 
                    TRIM(SPLIT_PART(address, E'\n', 1))
                WHEN POSITION(',' IN address) > 0 THEN 
                    TRIM(SPLIT_PART(address, ',', 1))
                ELSE TRIM(address)
            END AS store_address,
        address AS store_address_raw,

        TRIM(REGEXP_REPLACE(city, '[^\x20-\x7E]', '', 'g')) AS city,
        TRIM(REGEXP_REPLACE(state, '[^\x20-\x7E]', '', 'g')) AS state,
        TRIM(zip_code) AS zip_code,

        REGEXP_REPLACE(phone, '[^0-9x+()-]', '', 'g') AS store_phone,
        
        phone AS store_phone_raw,

        TRIM(manager) AS manager,
        UPPER(TRIM(store_type)) AS store_type,
        CASE 
                WHEN opening_date IS NULL OR TRIM(opening_date::VARCHAR)='' THEN NULL
                ELSE TRY_CAST(opening_date AS DATE)
                END as opening_date,
        
        CASE 
            WHEN TRY_CAST(opening_date AS DATE) IS NOT NULL 
                AND TRY_CAST(opening_date AS DATE) <= CURRENT_DATE 
            THEN DATE_DIFF('day', TRY_CAST(opening_date AS DATE), CURRENT_DATE)
            ELSE NULL
        END AS days_since_opening,

        CASE 
            WHEN TRY_CAST(opening_date AS DATE) IS NOT NULL 
                AND TRY_CAST(opening_date AS DATE) <= CURRENT_DATE 
            THEN ROUND(DATE_DIFF('day', TRY_CAST(opening_date AS DATE), CURRENT_DATE) / 365.25, 0)
            ELSE NULL
        END AS years_since_opening,

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

        CASE WHEN phone IS NULL OR TRIM(phone) = '' THEN 1 ELSE 0 END AS missing_phone_flag,
        CASE WHEN manager IS NULL OR TRIM(manager) = '' THEN 1 ELSE 0 END AS missing_manager_flag,

        CASE 
            WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) < 10 THEN 1
            ELSE 0
        END AS phone_too_short_flag,

        CASE WHEN store_name IS NULL OR TRIM(store_name) = '' THEN 1 ELSE 0 END AS missing_store_name_flag,
        CASE WHEN store_type IS NULL OR TRIM(store_type) = '' THEN 1 ELSE 0 END AS missing_store_type_flag,
        CASE WHEN opening_date IS NULL THEN 1 ELSE 0 END AS missing_opening_date_flag,

        CASE 
            WHEN TRY_CAST(opening_date AS DATE) > CURRENT_DATE THEN 1 
            ELSE 0 
        END AS future_opening_date_flag,

        CASE 
            WHEN TRY_CAST(opening_date AS DATE) IS NOT NULL 
                AND DATE_DIFF('year', TRY_CAST(opening_date AS DATE), CURRENT_DATE) > 20 
            THEN 1
            ELSE 0
        END AS very_old_store_flag,

        CASE 
            WHEN TRY_CAST(opening_date AS DATE) IS NULL THEN 'Unknown'
            WHEN TRY_CAST(opening_date AS DATE) > CURRENT_DATE THEN 'Not Yet Opened'
            WHEN DATE_DIFF('day', TRY_CAST(opening_date AS DATE), CURRENT_DATE) < 365 THEN 'New (< 1 year)'
            WHEN DATE_DIFF('day', TRY_CAST(opening_date AS DATE), CURRENT_DATE) < 1095 THEN 'Young (1-3 years)'
            WHEN DATE_DIFF('day', TRY_CAST(opening_date AS DATE), CURRENT_DATE) < 1825 THEN 'Established (3-5 years)'
            ELSE 'Mature (5+ years)'
        END AS store_age_category,

        CASE 
            WHEN TRY_CAST(opening_date AS DATE) <= CURRENT_DATE 
                AND phone IS NOT NULL 
                AND manager IS NOT NULL 
            THEN TRUE
            ELSE FALSE
        END AS is_fully_operational,

        CURRENT_TIMESTAMP AS _loaded_at,
        '{{ run_started_at }}' AS _batch_id,
        '{{ var("source_system", "RETAIL_S3") }}' AS _source_system,

        MD5(
            COALESCE(store_name, '') || '|' ||
            COALESCE(address, '') || '|' ||
            COALESCE(city, '') || '|' ||
            COALESCE(state, '') || '|' ||
            COALESCE(CAST(zip_code AS VARCHAR), '') || '|' ||
            COALESCE(phone, '') || '|' ||
            COALESCE(manager, '') || '|' ||
            COALESCE(store_type, '') || '|' ||
            COALESCE(CAST(opening_date AS VARCHAR), '')
        ) AS _row_hash,

        'raw_stores' AS _source_table

    FROM source_data
)

SELECT * FROM bronze_cleaned