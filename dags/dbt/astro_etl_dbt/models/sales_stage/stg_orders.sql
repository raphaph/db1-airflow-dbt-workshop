{{
    config(
        tags=["stage", "sales"],
        materialized="table"
    )
}}

select
    cast(order_id as varchar(70)) as order_id,
    cast(customer_id as varchar(70)) as customer_id,
    cast(order_status as varchar(30)) as order_status,
    cast(order_purchase_timestamp as timestamp without time zone) as order_purchase_timestamp

from {{ source('raw_data', 'tb_orders') }}
