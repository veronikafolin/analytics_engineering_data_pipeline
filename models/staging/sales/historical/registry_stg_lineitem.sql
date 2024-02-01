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
    from {{ref('snapshot_lineitem')}}
    where DATE(DBT_VALID_FROM) = (select MAX(DATE(DBT_VALID_FROM)) from {{ref('snapshot_lineitem')}})
),

previous_state_of_registry as (
    select *
    from {{this}}
    where partition_date = (select MAX(partition_date) from {{this}})
),

final as (
    select
        IFF(new.lineitemkey is null, old.lineitemkey, new.lineitemkey) as lineitemkey,
        IFF(new.lineitemkey is null, old.orderkey, new.l_orderkey) as orderkey,
        IFF(new.lineitemkey is null, old.linenumber, new.l_linenumber) as linenumber,
        IFF(new.lineitemkey is null, old.partkey, new.l_partkey) as partkey,
        IFF(new.lineitemkey is null, old.suppkey, new.l_suppkey) as suppkey,
        IFF(new.lineitemkey is null, old.partsuppkey, new.partsuppkey) as partsuppkey,
        CAST(IFF(new.lineitemkey is null, old.quantity, new.l_quantity) AS int) as quantity,
        IFF(new.lineitemkey is null, old.extendedprice, new.l_extendedprice) as extendedprice,
        IFF(new.lineitemkey is null, old.discount, new.l_discount) as discount,
        IFF(new.lineitemkey is null, old.tax, new.l_tax) as tax,
        IFF(new.lineitemkey is null, old.returnflag, new.l_returnflag) as returnflag,
        IFF(new.lineitemkey is null, old.linestatus, new.l_linestatus) as linestatus,
        IFF(new.lineitemkey is null, old.shipdate, new.l_shipdate) as shipdate,
        IFF(new.lineitemkey is null, old.commitdate, new.l_commitdate) as commitdate,
        IFF(new.lineitemkey is null, old.receiptdate, new.l_receiptdate) as receiptdate,
        IFF(new.lineitemkey is null, old.shipinstruct, new.l_shipinstruct) as shipinstruct,
        IFF(new.lineitemkey is null, old.shipmode, new.l_shipmode) as shipmode,
        CURRENT_DATE() as partition_date
    from last_snapshot as new FULL OUTER JOIN previous_state_of_registry as old ON new.lineitemkey = old.lineitemkey
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}
