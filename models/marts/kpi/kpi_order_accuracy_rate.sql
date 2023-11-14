with

orders as (
    select * from {{ref('fct_orders')}}
),

fulfilled_orders as (
    select orderkey
    from orders
    where orderstatus = 'F'
),

final as (
    select
    ((select count(orderkey) from fulfilled_orders) /
    (select count(orderkey) from orders)) * 100
    as order_accuracy_rate
)

select * from final