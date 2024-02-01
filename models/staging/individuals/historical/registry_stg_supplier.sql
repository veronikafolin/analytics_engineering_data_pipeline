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
    from {{ref('snapshot_supplier')}}
    where DATE(DBT_VALID_FROM) = (select MAX(DATE(DBT_VALID_FROM)) from {{ref('snapshot_supplier')}})
),

previous_state_of_registry as (
    select *
    from {{this}}
    where partition_date = (select MAX(partition_date) from {{this}})
),

final as (
    select
        IFF(new.s_suppkey is null, old.suppkey, new.s_suppkey) as suppkey,
        IFF(new.s_suppkey is null, old.supp_name, new.s_name) as supp_name,
        IFF(new.s_suppkey is null, old.nationkey, new.s_nationkey) as nationkey,
        IFF(new.s_suppkey is null, old.supp_acctbal, new.s_acctbal) as supp_acctbal,
        CURRENT_DATE() as partition_date
    from last_snapshot as new FULL OUTER JOIN previous_state_of_registry as old ON new.s_suppkey = old.suppkey
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}