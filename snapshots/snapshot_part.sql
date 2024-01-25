{% snapshot snapshot_part %}

    {{
        config(
          target_database='analytics',
          target_schema='snapshots',
          strategy='check',
          unique_key='p_partkey',
          check_cols='all',
        )
    }}

    select * from {{ source('raw', 'part') }}

{% endsnapshot %}