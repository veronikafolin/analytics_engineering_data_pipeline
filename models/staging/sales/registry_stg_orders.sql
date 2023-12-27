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
    from {{ref('snapshot_orders')}}
    where DATE(DBT_VALID_FROM) = (select MAX(DATE(DBT_VALID_FROM)) from {{ref('snapshot_orders')}})
),

previous_state_of_registry as (
    select *
    from {{this}}
    where partition_date = (select MAX(partition_date) from {{this}})
),

final as (
    select
        COALESCE(new.o_orderkey, old.orderkey) as orderkey,
        COALESCE(new.o_custkey, old.custkey) as custkey,
        COALESCE(new.o_orderstatus, old.orderstatus) as orderstatus,
        COALESCE(new.o_totalprice, old.totalprice) as totalprice,
        COALESCE(new.o_orderdate, old.orderdate) as orderdate,
        COALESCE(new.o_orderpriority, old.orderpriority) as orderpriority,
        COALESCE(new.o_clerk, old.clerk) as clerk,
        COALESCE(new.o_shippriority, old.shippriority) as shippriority,
        CURRENT_DATE() as partition_date
    from last_snapshot as new FULL OUTER JOIN previous_state_of_registry as old ON new.o_orderkey = old.orderkey
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}