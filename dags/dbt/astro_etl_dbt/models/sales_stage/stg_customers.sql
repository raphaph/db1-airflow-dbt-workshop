{{
    config(
        tags=["stage", "sales"],
        materialized="table"
    )
}}

select
    cast(customer_id as varchar(70)) as customer_id,
    cast(customer_unique_id as varchar(70)) as customer_unique_id,
    cast(customer_city as varchar(50)) as customer_city,
    cast(customer_state as varchar(2)) as customer_state

from {{ source('raw_data', 'tb_customers') }}
