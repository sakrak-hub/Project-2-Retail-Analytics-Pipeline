{{
    config(
        materialized='view'
    )
}}

WITH store_data AS(
select 
*
from
read_parquet('s3://my-retail-2026-analytics-5805/retail_data/stores.parquet')
),

store_metadata AS(
    SELECT 
    regexp_extract(filename, 'retail_data/(.*).parquet', 1) AS filename,
    size,
    last_modified::DATE as modified_date
    FROM read_blob('s3://my-retail-2026-analytics-5805/retail_data/*.parquet')
    WHERE regexp_extract(filename, 'retail_data/(.*).parquet', 1)='stores'
)

SELECT * 
FROM store_data CROSS JOIN store_metadata