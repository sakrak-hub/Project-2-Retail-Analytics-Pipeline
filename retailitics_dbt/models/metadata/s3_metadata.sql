{{
    config(
        materialized='view',
        unique_key='date_key',
        tags=['s3_metadata']
    )
}}

SELECT
strftime(stats_max::DATE, '%Y%m%d') as date_key,
file_name,
row_group_num_rows,
path_in_schema,
stats_max::DATE as transaction_date_in_s3
FROM parquet_metadata('s3://my-retail-2026-analytics-5805/retail_data/transactions/*.parquet')
WHERE path_in_schema = 'date';