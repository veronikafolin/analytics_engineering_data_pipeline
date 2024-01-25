{% snapshot snapshot_orders %}

    {{
        config(
          target_database='analytics',
          target_schema='snapshots',
          strategy='check',
          unique_key='o_orderkey',
          check_cols='all',
        )
    }}

    select *
    from {{ source('raw', 'orders') }}

{% endsnapshot %}