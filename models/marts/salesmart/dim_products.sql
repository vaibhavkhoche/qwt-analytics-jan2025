{{config(materialized = 'view', schema = env_var('DBT_SALESMART','SALESMART')}}

select 
* from
{{ref('trf_products')}}