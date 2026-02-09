{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        tags=['silver', 'staging']
    )
}}

WITH bronze AS (
    SELECT * FROM {{ ref('bronze_transactions') }}
    
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),