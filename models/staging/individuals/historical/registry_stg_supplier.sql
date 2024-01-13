{{
    config(
        cluster_by=['partition_date'],
        materialized='incremental',
        on_schema_change='append_new_columns',
        post_hook='delete from {{this}} {{apply_retention_mechanism(8)}}'
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
        COALESCE(new.s_suppkey, old.suppkey) as suppkey,
        COALESCE(new.s_name, old.supp_name) as supp_name,
        COALESCE(new.s_nationkey, old.nationkey) as nationkey,
        COALESCE(new.s_acctbal, old.supp_acctbal) as supp_acctbal,
        CURRENT_DATE() as partition_date
    from last_snapshot as new FULL OUTER JOIN previous_state_of_registry as old ON new.s_suppkey = old.suppkey
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}