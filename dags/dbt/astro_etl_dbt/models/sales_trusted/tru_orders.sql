{{
    config(
        tags=["stage", "sales"],
        materialized="incremental",
        unique_key="cd_pedido"
    )
}}

select
    order_id as cd_pedido,
    customer_id as cd_cliente,
    case 
        when order_status = 'approved' then 1
        when order_status = 'cancelled' then 2
        when order_status in ('processing', 'delivered') then 3
        else 0
    end as cd_status,
    order_status as ds_status,
    order_purchase_timestamp as dh_venda,
    cast(order_purchase_timestamp as date) as dt_venda

from sales_stage.stg_orders