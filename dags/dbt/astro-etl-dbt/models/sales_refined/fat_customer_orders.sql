{{ config(
    tags=["trusted", "customers"],
    materialized="incremental",
    unique_key="cd_cliente"
)}}

select 
    c.cd_cliente,
    c.cd_cliente_unico,
    c.ds_cidade,
    c.sg_estado,
    o.dh_venda,
    o.cd_pedido,
    o.cd_status

from sales_trusted.tru_customers as c
left join sales_trusted.tru_orders as o
on c.cd_cliente = o.cd_cliente