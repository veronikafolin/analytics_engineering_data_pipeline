with

orders as (
    select * from {{ref('stg_orders')}}
),

final as (
    select
        sum(totalprice)/count(orderkey) as average_order_value
    from orders
)

select * from final