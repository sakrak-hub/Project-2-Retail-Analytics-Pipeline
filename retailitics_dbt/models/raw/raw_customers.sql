{{
    config(
        materialized='view'
    )
}}

WITH customer_data AS(
    SELECT 
    *
    FROM
    read_parquet('s3://my-retail-2026-analytics-5805/retail_data/customers.parquet')
),

customer_metadata AS(
    SELECT 
    regexp_extract(filename, 'retail_data/(.*).parquet', 1) AS filename,
    size,
    last_modified::DATE as modified_date
    FROM read_blob('s3://my-retail-2026-analytics-5805/retail_data/*.parquet')
    WHERE regexp_extract(filename, 'retail_data/(.*).parquet', 1)='customers'
)

SELECT * FROM customer_data CROSS JOIN customer_metadata