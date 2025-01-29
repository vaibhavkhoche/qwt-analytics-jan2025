{% snapshot shipments_snapshot %}
{{
    config
    (
        target_database =  env_var('DBT_DBNAME', 'QWT_ANALYTICS'),
        target_schema = env_var('DBT_SNAPSHOTSCHEMA', 'SNAPSHOT'),
        unique_key = "orderid||'-'||lineno",

        strategy = 'timestamp',
        updated_at = 'Shipmentdate'
    )
}}
select * 
from
{{ref('stg_shipments')}}
{% endsnapshot %}