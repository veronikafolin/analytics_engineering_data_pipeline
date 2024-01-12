{{config (
    cluster_by=['partition_date'],
    materialized='incremental',
    on_schema_change='append_new_columns'
)}}

with

lineitem as (
    select *
    from {{ref('registry_stg_lineitem')}}
    where partition_date = (select MAX(partition_date) from {{ref('registry_stg_lineitem')}})
),

orders as (
    select *
    from {{ref('fct_orders')}}
    where partition_date = (select MAX(partition_date) from {{ref('fct_orders')}})
),

supplier as (
    select *
    from {{ref('dim_supplier')}}
    where partition_date = (select MAX(partition_date) from {{ref('dim_supplier')}})
),

inventory as (
    select *
    from {{ref('fct_inventory')}}
     where partition_date = (select MAX(partition_date) from {{ref('fct_inventory')}})
),

final as (
    select
        lineitem.lineitemkey,
        lineitem.orderkey,
        lineitem.partsuppkey,
        orders.custkey,
        orders.cust_mktsegment,
        orders.cust_nation_name,
        orders.cust_region_name,
        orders.orderdate,
        orders.orderstatus,
        orders.clerk,
        supplier.suppkey,
        supplier.supp_name,
        supplier.supp_nation_name,
        supplier.supp_region_name,
        lineitem.quantity,
        lineitem.extendedprice,
        lineitem.discount,
        lineitem.tax,
        lineitem.returnflag,
        lineitem.linestatus,
        lineitem.shipdate,
        lineitem.commitdate,
        lineitem.receiptdate,
        lineitem.shipmode,
        {{ compute_discounted_extended_price('extendedprice', 'discount') }} as net_revenue,
        {{ compute_discounted_extended_price_plus_tax('extendedprice', 'discount', 'tax') }} as gross_revenue,
        {{ compute_cost_of_good_sold(supplycost, quantity) }} as cost_of_good_sold,
        {{ compute_profit(net_revenue, supplycost, quantity) }} as profit,
        CURRENT_DATE() as partition_date
    from lineitem
    join orders using(orderkey)
    join supplier using(suppkey)
    join inventory using(partsuppkey)
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}