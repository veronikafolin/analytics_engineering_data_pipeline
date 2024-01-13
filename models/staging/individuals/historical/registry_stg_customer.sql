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
    from {{ref('snapshot_customer')}}
    where DATE(DBT_VALID_FROM) = (select MAX(DATE(DBT_VALID_FROM)) from {{ref('snapshot_customer')}})
),

previous_state_of_registry as (
    select *
    from {{this}}
    where partition_date = (select MAX(partition_date) from {{this}})
),

final as (
    select
            COALESCE(new.c_custkey, old.custkey) as custkey,
            COALESCE(new.c_name, old.cust_name) as cust_name,
            COALESCE(new.c_nationkey, old.nationkey) as nationkey,
            COALESCE(new.c_acctbal, old.cust_acctbal) as cust_acctbal,
            COALESCE(new.c_mktsegment, old.cust_mktsegment) as cust_mktsegment,
            CURRENT_DATE() as partition_date
        from last_snapshot as new FULL OUTER JOIN previous_state_of_registry as old ON new.c_custkey = old.custkey
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}