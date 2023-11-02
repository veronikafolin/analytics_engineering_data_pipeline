with orders as (
    select * from {{ref('stg_orders')}}
),
customer as (
    select * from {{ref('stg_customer')}}
),
final as (
    select
        o.o_orderkey,
        c.c_name,
        o.o_orderstatus,
        o.o_totalprice,
        o.o_orderdate,
        o.o_orderpriority,
        o.o_clerk,
        o.o_shippriority
    from orders as o
    join customer as c on c.c_custkey = o.o_custkey
)

select * from final