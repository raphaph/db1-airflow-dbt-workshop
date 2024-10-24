{{ config(
    tags=["stage", "customers"],
    materialized="table"
)}}

select 
    cast(customer_id as varchar(100)) as customer_id,
    cast(customer_unique_id as varchar(100)) as customer_unique_id,
    cast(customer_zip_code_prefix as bigint) as customer_zip_code_prefix,
    cast(customer_city as varchar(100)) as customer_city,
    cast(customer_state as varchar(20)) as customer_state

from {{ source('raw_data','tb_customers')}}