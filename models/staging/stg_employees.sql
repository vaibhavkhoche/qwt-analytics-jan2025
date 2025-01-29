{{ config(materialized = 'table',pre_hook = 'use warehouse compute_wh;') }}

select * from 
{{source('qwt_raw','raw_employees')}}