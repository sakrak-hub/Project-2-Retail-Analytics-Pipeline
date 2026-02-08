{{
    config(
        materialized='view'
    )
}}

select 
customer_id,          
first_name,           
last_name,            
email,                
phone ,               
address,              
city,                 
state,                
zip_code,             
date_of_birth,        
gender,               
registration_date,    
loyalty_member,       
preferred_contact,    
customer_segment,     
total_lifetime_value 
from
read_parquet('s3://my-retail-2026-analytics-5805/retail_data/customers.parquet')