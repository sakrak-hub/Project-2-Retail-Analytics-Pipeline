{{
    config(
        materialized='view'
    )
}}

SELECT 
r.transaction_id,
m.modified_date AS _loaded_at_date,
r.date,                  
r.time,                  
r.datetime,             
r.customer_id,           
r.store_id,              
r.store_name,            
r.cashier_id,            
r.payment_method,        
r.subtotal,              
r.tax_amount,            
r.total_amount,          
r.items_count,           
r.loyalty_points_earned, 
r.promotion_code,        
r.refund_reason,         
r.status,                
r.product_id,            
r.product_name,          
r.category,              
r.quantity,              
r.unit_price,            
r.discount_percent,      
r.line_total
from
read_parquet('s3://my-retail-2026-analytics-5805/retail_data/transactions/*.parquet') r
INNER JOIN {{ ref('s3_metadata') }} m
ON regexp_extract(r.transaction_id, 'TXN(\d{8})', 1) = m.date_key