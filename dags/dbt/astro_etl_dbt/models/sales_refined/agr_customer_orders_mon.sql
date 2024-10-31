{{
    config(
        tags=["stage", "sales"],
        materialized="incremental",
        unique_key="cd_cliente"
    )
}}

select
    sg_estado,
    cast(to_char(dt_venda, 'YYYYMM') as bigint) as cd_periodo_mes,
    ds_status,
    count(distinct cd_cliente) as qt_cliente_distinto,
    count(cd_pedido) as qt_pedidos

from sales_refined.fat_customer_orders_day
group by 1,2,3