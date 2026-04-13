{{
    config(
        materialized='view'
    )
}}

WITH phone_extract AS(
        SELECT
        customer_id,
        CASE
        WHEN '+1' IN regexp_replace(phone, '[-().]','','g') OR  STARTS_WITH(regexp_replace(phone, '[-().]','','g'),'001') THEN '+1'
        ELSE NULL 
        END AS country_code,

        CASE 
        WHEN 'x' IN phone THEN split_part(phone, 'x', 2)
        ELSE NULL
        END AS extension,

        regexp_replace(phone, '[-().]','','g') AS cleaned_phone,

        phone as phone_unedited
        FROM {{ ref('raw_customers') }}
    )

    SELECT 
    customer_id,
    phone_unedited,
    country_code,
    CASE
        WHEN LENGTH(cleaned_phone) <= 10 
            AND cleaned_phone NOT LIKE '+%'
            AND cleaned_phone NOT LIKE '001%'
            AND cleaned_phone NOT LIKE '%x%'
        THEN cleaned_phone
        
        WHEN (cleaned_phone LIKE '+1%' OR cleaned_phone LIKE '001%') 
            AND cleaned_phone LIKE '%x%'
        THEN regexp_extract(cleaned_phone, '^(?:\+1|001)(\d+?)x', 1)
        
        WHEN (cleaned_phone LIKE '+1%' OR cleaned_phone LIKE '001%')
            AND cleaned_phone NOT LIKE '%x%'
        THEN regexp_replace(cleaned_phone, '^(?:\+1|001)', '')
        
        WHEN cleaned_phone LIKE '%x%'
            AND cleaned_phone NOT LIKE '+%'
            AND cleaned_phone NOT LIKE '001%'
        THEN regexp_extract(cleaned_phone, '^(\d+?)x', 1)
        
        ELSE cleaned_phone
    END AS extracted_phone,
    cleaned_phone,
    extension
    FROM phone_extract