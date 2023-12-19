{{
    config(
        materialized='incremental'
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

--final as (
--    select
--        IFF(new.o_orderkey is null, new.o_orderkey, old.orderkey) as orderkey,
--        IFF(new.o_orderkey is null, new.o_custkey, old.custkey) as custkey,
--        IFF(new.o_orderkey is null, new.o_orderstatus, old.orderstatus) as orderstatus,
--        IFF(new.o_orderkey is null, new.o_totalprice, old.totalprice) as totalprice,
--        IFF(new.o_orderkey is null, new.o_orderdate, old.orderdate) as orderdate,
--        IFF(new.o_orderkey is null, new.o_orderpriority, old.orderpriority) as orderpriority,
--        IFF(new.o_orderkey is null, new.o_clerk, old.clerk) as clerk,
--        IFF(new.o_orderkey is null, new.o_shippriority, old.shippriority) as shippriority,
--        CURRENT_DATE() as partition_date
--    from last_snapshot as new FULL OUTER JOIN previous_state_of_registry as old ON new.o_orderkey = old.orderkey
--    order by new.o_orderkey
--    LIMIT 20
--)

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
    order by o_orderkey
    LIMIT 20
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}