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
            IFF(new.c_custkey is null, old.custkey, new.c_custkey) as custkey,
            IFF(new.c_custkey is null, old.cust_name, new.c_name) as cust_name,
            IFF(new.c_custkey is null, old.nationkey, new.c_nationkey) as nationkey,
            IFF(new.c_custkey is null, old.cust_acctbal, new.c_acctbal) as cust_acctbal,
            IFF(new.c_custkey is null, old.cust_mktsegment, new.c_mktsegment) as cust_mktsegment,
            CURRENT_DATE() as partition_date
        from last_snapshot as new FULL OUTER JOIN previous_state_of_registry as old ON new.c_custkey = old.custkey
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}