{{
    config(
        tags=["stage", "sales"],
        materialized="incremental",
        unique_key="cd_cliente"
    )
}}

select
    customer_id as cd_cliente,
    customer_unique_id as cd_cliente_unico,
    customer_city as ds_cidade,
    customer_state as sg_estado

from sales_stage.stg_customers
