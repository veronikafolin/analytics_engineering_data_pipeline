with

orders as (
    select * from {{ref('fct_orders')}}
),

final as (
    select
        sum(totalprice)/count(orderkey) as average_order_value
    from orders
)

select * from final