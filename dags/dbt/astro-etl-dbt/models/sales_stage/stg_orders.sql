{{ config(
    tags=["stage", "orders"],
    materialized="table"
)}}

select 
    cast(order_id as varchar(150)) as order_id,
    cast(customer_id as varchar(150)) as customer_id,
    cast(order_status as varchar(100)) as order_status,
    cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp,
    cast(order_approved_at as timestamp) as order_approved_at,
    cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date,
    cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date
    
from {{ source('raw_data','tb_orders')}}