{{
    config(
        materialized='view',
        unique_key='date_key',
        tags=['s3_metadata']
    )
}}

SELECT 
replace(regexp_extract(filename, '\d{4}-\d{2}-\d{2}'), '-', '') AS date_key,
regexp_extract(filename, '\d{4}-\d{2}-\d{2}')::DATE AS transaction_date,
size,
last_modified::DATE as modified_date
FROM read_blob('s3://my-retail-2026-analytics-5805/retail_data/transactions/*.parquet')