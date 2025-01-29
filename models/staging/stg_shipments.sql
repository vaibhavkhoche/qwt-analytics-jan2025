{{ config(materialized = 'table') }}

select 
OrderID,
LineNo,
ShipperID,
CustomerID,
ProductID,
EmployeeID,
TO_DATE(SPLIT_PART(ShipmentDate,' ', 1)) as ShipmentDate, 
Status,
from 
{{source('qwt_raw','raw_shipments')}}