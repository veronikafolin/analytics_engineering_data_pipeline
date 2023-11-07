with

orders as (
    select * from {{ref('fct_orders')}}
),

final as (
    select
        custkey,
        min(orderdate) as first_order_date,
        max(orderdate) as most_recent_order_date,
        count(orderkey) as number_of_orders,
        sum(totalprice) as sum_total_price
    from orders
    group by 1
)

select * from final