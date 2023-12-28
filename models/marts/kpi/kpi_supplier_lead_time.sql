with

sales as (
    select * from {{ref('fct_sales')}}
),

delivery_time as (
    select
        shipdate,
        receiptdate,
        datediff(day, shipdate, receiptdate) as delivery_days
    from sales
),

final as (
    select avg(delivery_days) as avg_delivery_days
    from delivery_time
)

select * from final