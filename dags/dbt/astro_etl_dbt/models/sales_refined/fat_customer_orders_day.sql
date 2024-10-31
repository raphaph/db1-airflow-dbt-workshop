{{
    config(
        tags=["stage", "sales"],
        materialized="incremental",
        unique_key="cd_pedido"
    )
}}

select 
    o.dh_venda,
    o.dt_venda,
    o.cd_pedido,
    c.cd_cliente,
    c.cd_cliente_unico,
    c.sg_estado,
    o.ds_status

from sales_trusted.tru_customers as c

left join sales_trusted.tru_orders as o
on c.cd_cliente = o.cd_cliente