with customer as (
    select * from {{ref('stg_customer')}}
),
orders as (
    select * from {{ref('stg_orders')}}
),
customer_orders as (
    select
        o_custkey,
        min(o_orderdate) as first_order_date,
        max(o_orderdate) as most_recent_order_date,
        count(o_orderkey) as number_of_orders,
        sum(o_totalprice) as sum_total_price
    from orders
    group by 1
),
final as (
    select
        customer.c_custkey,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        customer_orders.sum_total_price
    from customer
    left join customer_orders on customer_orders.o_custkey = customer.c_custkey
)
select * from final