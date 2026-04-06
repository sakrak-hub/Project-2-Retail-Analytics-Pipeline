{{
    config(
        materialized='view',
        unique_key='filename',
        tags=['s3_metadata']
    )
}}

SELECT 
regexp_extract(filename, 'retail_data/(.*).parquet', 1) as filename,
size,
last_modified::DATE as modified_date
FROM read_blob('s3://my-retail-2026-analytics-5805/retail_data/*.parquet')