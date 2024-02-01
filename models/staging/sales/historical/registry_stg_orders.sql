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
        IFF(new.o_orderkey is null, old.orderkey, new.o_orderkey) as orderkey,
        IFF(new.o_orderkey is null, old.custkey, new.o_custkey) as custkey,
        IFF(new.o_orderkey is null, old.orderstatus, new.o_orderstatus) as orderstatus,
        IFF(new.o_orderkey is null, old.totalprice, new.o_totalprice) as totalprice,
        IFF(new.o_orderkey is null, old.orderdate, new.o_orderdate) as orderdate,
        IFF(new.o_orderkey is null, old.orderpriority, new.o_orderpriority) as orderpriority,
        IFF(new.o_orderkey is null, old.clerk, new.o_clerk) as clerk,
        IFF(new.o_orderkey is null, old.shippriority, new.o_shippriority) as shippriority,
        CURRENT_DATE() as partition_date
    from last_snapshot as new FULL OUTER JOIN previous_state_of_registry as old ON new.o_orderkey = old.orderkey
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}