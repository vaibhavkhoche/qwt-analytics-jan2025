{{ config(materialized ='table', schema = env_var('DBT_TRANSFORMINGSCHEMA','TRANSFORMING')) }}
 
select
o.orderid,
od.lineno,
od.productid,
o.customerid,
o.employeeid,
o.shipperid,
o.freight,
od.quantity,
od.unitprice,
od.discount,
od.orderdate,
 
to_decimal((od.unitprice * od.quantity) * (1-od.discount),9,2) as linesalesamount,
to_decimal(p.unitcost * od.quantity, 9,2) as costofgoodssold,
to_decimal(((od.unitprice * od.quantity) * (1 - od.discount)) -(p.unitcost * od.quantity),9,2) as margin
 
from
{{ref('stg_orders')}} as o inner join {{ref('stg_order_details')}} as od
on o.orderid = od.orderid
 
inner join
 
{{ref('stg_products')}} as p on p.productid =od.productid