{{config(materialized = 'table', schema = env_var('DBT_TRANSFORMINGSCHEMA','TRANSFORMING'))}}
 
with recursive managers (indent, officeid, employeeid, managerid, employeetitle, managertitle)
as
(
    select ' ' as indent, office as officeid, empid as employeeid, reportsto as managerid, title as employeetitle, title as managertitle
    from
    {{ref('stg_employees')}}
    where title = 'President'
 
    union all
 
    select indent || '* ', employees.office as officeid, employees.empid as employeeid, employees.reportsto as managerid, employees.title
    as employeetitle, mgr.title as maangertitle
 
    from
 
    {{ref("stg_employees")}} as employees join managers
    on employees.reportsto = managers.employeeid join {{ref('stg_employees')}} as mgr
    on employees.reportsto = mgr.empid
)
,
 
offices as
(
    select office,
           address,
           city,
           country
    from
    {{ref('stg_offices')}}
)
 
select indent || employeetitle as title, managers.officeid, offices.address, offices.city, offices.country, employeeid, managerid, managertitle
from managers join offices on managers.officeid = offices.office