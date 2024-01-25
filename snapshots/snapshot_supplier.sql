{% snapshot snapshot_supplier %}

    {{
        config(
          target_database='analytics',
          target_schema='snapshots',
          strategy='check',
          unique_key='s_suppkey',
          check_cols='all',
        )
    }}

    select * from {{ source('raw', 'supplier') }}

{% endsnapshot %}