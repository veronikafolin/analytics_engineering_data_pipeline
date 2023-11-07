with

orders as (
    select * from {{ref('stg_orders')}}
),

customer as (
    select * from {{ref('dim_customer')}}
),

final as (
    select *
    from orders
    join customer using(custkey)
)

select * from final