{% snapshot orders_snapshot %}

    {{
        config(
          target_database='analytics',
          target_schema='snapshots',
          strategy='check',
          unique_key='o_orderkey',
          check_cols=['orderstatus'],
        )
    }}

    select * from {{ source('tpch_sf1', 'orders') }}

{% endsnapshot %}