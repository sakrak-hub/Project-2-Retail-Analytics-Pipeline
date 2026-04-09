{{
    config(
        materialized='incremental',
        unique_key='store_id',
        on_schema_change='fail',
        tags=['staging', 'incremental']
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

store_name_clean AS(
    WITH stores AS(
        SELECT DISTINCT(store_name) AS store_name_raw,
        store_id
        FROM {{ ref('raw_stores')}}
    )

    SELECT
    store_id,
    TRIM(REGEXP_REPLACE(store_name_raw, '[^\x20-\x7E]', '', 'g')) AS store_name_clean,
    CASE 
    WHEN store_name_raw = TRIM(REGEXP_REPLACE(store_name_raw, '[^\x20-\x7E]', '', 'g')) THEN 'Clean'
    ELSE 'Processed'
    END AS processed_flag
    FROM stores
    WHERE processed_flag='Clean'
    ORDER BY 1
),

staging_cleaned AS (
    SELECT
        sd.store_id,
        sc.store_name_clean AS store_name,
        sd.store_name AS store_name_raw,

        CASE 
                WHEN sd.address IS NULL OR TRIM(sd.address) = '' THEN NULL
                WHEN POSITION(E'\n' IN sd.address) > 0 THEN 
                    TRIM(SPLIT_PART(sd.address, E'\n', 1))
                WHEN POSITION(',' IN sd.address) > 0 THEN 
                    TRIM(SPLIT_PART(sd.address, ',', 1))
                ELSE TRIM(sd.address)
            END AS store_address,
        sd.address AS store_address_raw,

        TRIM(REGEXP_REPLACE(sd.city, '[^\x20-\x7E]', '', 'g')) AS city,
        TRIM(REGEXP_REPLACE(sd.state, '[^\x20-\x7E]', '', 'g')) AS state,
        TRIM(sd.zip_code) AS zip_code,

        REGEXP_REPLACE(sd.phone, '[^0-9x+()-]', '', 'g') AS store_phone,
        
        sd.phone AS store_phone_raw,

        TRIM(sd.manager) AS manager,
        UPPER(TRIM(sd.store_type)) AS store_type,
        CASE 
                WHEN sd.opening_date IS NULL OR TRIM(opening_date::VARCHAR)='' THEN NULL
                ELSE TRY_CAST(sd.opening_date AS DATE)
                END as opening_date,
        
        CASE 
            WHEN TRY_CAST(sd.opening_date AS DATE) IS NOT NULL 
                AND TRY_CAST(sd.opening_date AS DATE) <= CURRENT_DATE 
            THEN DATE_DIFF('day', TRY_CAST(sd.opening_date AS DATE), CURRENT_DATE)
            ELSE NULL
        END AS days_since_opening,

        CASE 
            WHEN TRY_CAST(sd.opening_date AS DATE) IS NOT NULL 
                AND TRY_CAST(sd.opening_date AS DATE) <= CURRENT_DATE 
            THEN ROUND(DATE_DIFF('day', TRY_CAST(sd.opening_date AS DATE), CURRENT_DATE) / 365.25, 0)
            ELSE NULL
        END AS years_since_opening,

        CASE 
            WHEN sd.address IS NULL THEN 0
            WHEN sd.city IS NOT NULL AND address NOT LIKE '%' || sd.city || '%' THEN 1
            ELSE 0
        END AS address_city_mismatch_flag,
        
        CASE 
            WHEN sd.address IS NULL THEN 0
            WHEN sd.state IS NOT NULL AND sd.address NOT LIKE '%' || sd.state || '%' THEN 1
            ELSE 0
        END AS address_state_mismatch_flag,
        
        CASE 
            WHEN sd.address IS NULL THEN 0
            WHEN sd.zip_code IS NOT NULL AND sd.address NOT LIKE '%' || sd.zip_code || '%' THEN 1
            ELSE 0
        END AS address_zip_mismatch_flag,

        CASE WHEN sd.city IS NULL OR TRIM(city) = '' THEN 1 ELSE 0 END AS missing_city_flag,
        CASE WHEN sd.state IS NULL OR TRIM(state) = '' THEN 1 ELSE 0 END AS missing_state_flag,
        CASE WHEN sd.zip_code IS NULL OR TRIM(zip_code) = '' THEN 1 ELSE 0 END AS missing_zip_code_flag,

        CASE WHEN sd.phone IS NULL OR TRIM(phone) = '' THEN 1 ELSE 0 END AS missing_phone_flag,
        CASE WHEN sd.manager IS NULL OR TRIM(manager) = '' THEN 1 ELSE 0 END AS missing_manager_flag,

        CASE 
            WHEN LENGTH(REGEXP_REPLACE(sd.phone, '[^0-9]', '', 'g')) < 10 THEN 1
            ELSE 0
        END AS phone_too_short_flag,

        CASE WHEN sd.store_name IS NULL OR TRIM(store_name) = '' THEN 1 ELSE 0 END AS missing_store_name_flag,
        CASE WHEN sd.store_type IS NULL OR TRIM(store_type) = '' THEN 1 ELSE 0 END AS missing_store_type_flag,
        CASE WHEN sd.opening_date IS NULL THEN 1 ELSE 0 END AS missing_opening_date_flag,

        CASE 
            WHEN TRY_CAST(sd.opening_date AS DATE) > CURRENT_DATE THEN 1 
            ELSE 0 
        END AS future_opening_date_flag,

        CASE 
            WHEN TRY_CAST(sd.opening_date AS DATE) IS NOT NULL 
                AND DATE_DIFF('year', TRY_CAST(sd.opening_date AS DATE), CURRENT_DATE) > 20 
            THEN 1
            ELSE 0
        END AS very_old_store_flag,

        CASE 
            WHEN TRY_CAST(sd.opening_date AS DATE) IS NULL THEN 'Unknown'
            WHEN TRY_CAST(sd.opening_date AS DATE) > CURRENT_DATE THEN 'Not Yet Opened'
            WHEN DATE_DIFF('day', TRY_CAST(sd.opening_date AS DATE), CURRENT_DATE) < 365 THEN 'New (< 1 year)'
            WHEN DATE_DIFF('day', TRY_CAST(sd.opening_date AS DATE), CURRENT_DATE) < 1095 THEN 'Young (1-3 years)'
            WHEN DATE_DIFF('day', TRY_CAST(sd.opening_date AS DATE), CURRENT_DATE) < 1825 THEN 'Established (3-5 years)'
            ELSE 'Mature (5+ years)'
        END AS store_age_category,

        CASE 
            WHEN TRY_CAST(sd.opening_date AS DATE) <= CURRENT_DATE 
                AND sd.phone IS NOT NULL 
                AND sd.manager IS NOT NULL 
            THEN TRUE
            ELSE FALSE
        END AS is_fully_operational,

        sd.modified_date AS raw_loaded_at,
        CURRENT_TIMESTAMP AS staging_loaded_at,
        '{{ run_started_at }}' AS staging_batch_id,
        '{{ var("source_system", "RETAIL_S3") }}' AS _source_system,

        MD5(
            COALESCE(sc.store_name_clean, '') || '|' ||
            COALESCE(sd.address, '') || '|' ||
            COALESCE(sd.city, '') || '|' ||
            COALESCE(sd.state, '') || '|' ||
            COALESCE(CAST(sd.zip_code AS VARCHAR), '') || '|' ||
            COALESCE(sd.phone, '') || '|' ||
            COALESCE(sd.manager, '') || '|' ||
            COALESCE(sd.store_type, '') || '|' ||
            COALESCE(CAST(sd.opening_date AS VARCHAR), '')
        ) AS _row_hash,

        'raw_stores' AS _source_table

    FROM source_data sd 
    INNER JOIN store_name_clean sc 
    ON sd.store_id = sc.store_id 
)

SELECT * FROM staging_cleaned