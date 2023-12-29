{{
    config(
        cluster_by=['partition_date'],
        materialized='incremental',
        on_schema_change='append_new_columns'
    )
}}

with

last_snapshot as (
    select *
    from {{ref('snapshot_partsupp')}}
    where DATE(DBT_VALID_FROM) = (select MAX(DATE(DBT_VALID_FROM)) from {{ref('snapshot_partsupp')}})
),

previous_state_of_registry as (
    select *
    from {{this}}
    where partition_date = (select MAX(partition_date) from {{this}})
),

final as (
    select
        COALESCE(new.partsuppkey, old.partsuppkey) as partsuppkey,
        COALESCE(new.ps_partkey, old.partkey) as partkey,
        COALESCE(new.ps_suppkey, old.suppkey) as suppkey,
        COALESCE(new.ps_availqty, old.availqty) as availqty,
        COALESCE(new.ps_supplycost, old.supplycost) as supplycost,
        CURRENT_DATE() as partition_date
    from last_snapshot as new FULL OUTER JOIN previous_state_of_registry as old ON new.partsuppkey = old.partsuppkey
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}