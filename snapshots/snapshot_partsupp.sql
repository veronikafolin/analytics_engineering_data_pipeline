{% snapshot snapshot_partsupp %}

    {{
        config(
          target_database='analytics',
          target_schema='snapshots',
          strategy='check',
          unique_key='partsuppkey',
          check_cols='all',
        )
    }}

    select *, {{ dbt_utils.generate_surrogate_key(['ps_partkey', 'ps_suppkey'])}} as partsuppkey
    from {{ source('raw', 'partsupp') }}

{% endsnapshot %}