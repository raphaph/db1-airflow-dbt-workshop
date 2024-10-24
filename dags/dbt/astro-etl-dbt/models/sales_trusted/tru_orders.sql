{{ config(
    tags=["stage", "orders"],
    materialized="incremental",
    unique_key="cd_pedido"
)}}

select 
    order_id as cd_pedido,
    customer_id as cd_cliente,
    order_status as cd_status,
    order_purchase_timestamp as dh_venda,
    order_approved_at as dh_aprovacao,
    order_delivered_customer_date as dh_entrega,
    order_estimated_delivery_date as dh_estimativa_entrega

from sales_stage.stg_orders