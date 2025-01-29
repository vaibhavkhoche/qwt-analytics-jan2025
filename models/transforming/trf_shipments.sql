{{ config(materialized ='table', schema = env_var('DBT_TRANSFORMINGSCHEMA','TRANSFORMING')) }}

select
sh.OrderID,
sh.lineno,
sp.CompanyName,
sh.Shipmentdate,
sh.Status
from
{{ref('shipments_snapshot')}} as sh
left join {{ref('lkp_shippers')}} as sp
on sh.ShipperID = sp.ShipperID
where sh.dbt_valid_to is null