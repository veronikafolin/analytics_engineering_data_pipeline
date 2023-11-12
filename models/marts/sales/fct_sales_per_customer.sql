with

lineitem as (
    select * from {{ref('stg_lineitem')}}
),

orders as (
    select * from {{ref('fct_orders')}}
),

final as (
    select
        orders.custkey,
        orders.cust_mktsegment,
        orders.cust_nation_name,
        orders.cust_region_name,
        lineitem.orderkey,
        lineitem.partsuppkey,
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
        {{ compute_discounted_extended_price('extendedprice', 'discount') }} as discounted_extended_price,
        {{ compute_discounted_extended_price_plus_tax('extendedprice', 'discount', 'tax') }} as discounted_extended_price_plus_tax
    from lineitem
    join orders using(orderkey)
    order by orders.orderdate
)

select * from final