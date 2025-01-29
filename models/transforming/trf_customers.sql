{{ config(materialized = 'table', schema = env_var('DBT_TRANSFORMINGSCHEMA','TRANSFORMING')  ) }}

select 
sc.customerid,
sc.companyname,
sc.contactname,
sc.city,
sc.country,
ld.divisionname,
sc.address,
sc.fax,
sc.phone,
sc.postalcode,
iff(sc.stateprovince = '','NA',sc.stateprovince)as statename
from
{{ref('stg_customers')}} as sc inner join
{{ref('lkp_divisions')}} as ld 
on sc.divisionid =  ld.divisionid

