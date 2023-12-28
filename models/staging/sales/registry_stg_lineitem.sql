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
        COALESCE(new.lineitemkey, old.lineitemkey) as lineitemkey,
        COALESCE(new.l_orderkey, old.orderkey) as orderkey,
        COALESCE(new.l_linenumber, old.linenumber) as linenumber,
        COALESCE(new.l_partkey, old.partkey) as partkey,
        COALESCE(new.l_suppkey, old.suppkey) as suppkey,
        COALESCE(new.partsuppkey, old.partsuppkey) as partsuppkey,
        CAST(COALESCE(new.l_quantity, old.quantity) AS int) as quantity,
        COALESCE(new.l_extendedprice, old.extendedprice) as extendedprice,
        COALESCE(new.l_discount, old.discount) as discount,
        COALESCE(new.l_tax, old.tax) as tax,
        COALESCE(new.l_returnflag, old.returnflag) as returnflag,
        COALESCE(new.l_linestatus, old.linestatus) as linestatus,
        COALESCE(new.l_shipdate, old.shipdate) as shipdate,
        COALESCE(new.l_commitdate, old.commitdate) as commitdate,
        COALESCE(new.l_receiptdate, old.receiptdate) as receiptdate,
        COALESCE(new.l_shipinstruct, old.shipinstruct) as shipinstruct,
        COALESCE(new.l_shipmode, old.shipmode) as shipmode,
        CURRENT_DATE() as partition_date
    from last_snapshot as new FULL OUTER JOIN previous_state_of_registry as old ON new.lineitemkey = old.lineitemkey
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}
