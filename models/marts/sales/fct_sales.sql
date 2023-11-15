with

lineitem as (
    select * from {{ref('stg_lineitem')}}
),

orders as (
    select * from {{ref('fct_orders')}}
),

final as (
    select
        lineitem.lineitemkey,
        lineitem.orderkey,
        lineitem.partsuppkey,
        orders.orderdate,
        orders.orderstatus,
        orders.clerk,
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
)

select * from final