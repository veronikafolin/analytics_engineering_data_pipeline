{% snapshot snapshot_customer %}

    {{
        config(
          target_database='analytics',
          target_schema='snapshots',
          strategy='check',
          unique_key='c_custkey',
          check_cols='all',
        )
    }}

    select * from {{ source('raw', 'customer') }}

{% endsnapshot %}