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
        customer.cust_region_name
    from orders
    join customer using(custkey)
)

select * from final