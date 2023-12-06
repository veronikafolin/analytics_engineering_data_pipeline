{{config (
    cluster_by=['valid_from']
)}}

with

orders as (
    select * from {{ref('stg_orders')}}
),

customer as (
    select * from {{ref('dim_customer')}}
),

final as (
    select
        orders.orderkey,
        orders.orderstatus,
        orders.totalprice,
        orders.orderdate,
        orders.orderpriority,
        orders.clerk,
        orders.shippriority,
        customer.custkey,
        customer.cust_mktsegment,
        customer.cust_nation_name,
        customer.cust_region_name,
        orders.valid_from,
        orders.valid_to
    from orders
    join customer using(custkey)
)

select * from final