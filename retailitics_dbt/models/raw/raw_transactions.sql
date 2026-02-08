{{ config(
    materialized='view'
) }}

select 
transaction_id,
date,                  
time,                  
datetime,             
customer_id,           
store_id,              
store_name,            
cashier_id,            
payment_method,        
subtotal,              
tax_amount,            
total_amount,          
items_count,           
loyalty_points_earned, 
promotion_code,        
refund_reason,         
status,                
product_id,            
product_name,          
category,              
quantity,              
unit_price,            
discount_percent,      
line_total  
from
read_parquet('s3://my-retail-2026-analytics-5805/retail_data/transactions/*.parquet')