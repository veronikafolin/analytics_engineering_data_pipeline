{% snapshot snapshot_lineitem %}

    {{
        config(
          target_database='analytics',
          target_schema='snapshots',
          strategy='check',
          unique_key='lineitemkey',
          check_cols='all'
        )
    }}

    select *,
    {{ dbt_utils.generate_surrogate_key(['l_orderkey', 'l_linenumber'])}} as lineitemkey,
    {{ dbt_utils.generate_surrogate_key(['l_partkey', 'l_suppkey'])}} as partsuppkey
    from {{ source('raw', 'lineitem') }}

{% endsnapshot %}