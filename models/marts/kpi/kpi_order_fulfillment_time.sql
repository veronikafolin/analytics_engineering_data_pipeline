with

sales as (
    select * from {{ref('fct_sales')}}
),

fulfillment_time as (
    select
        orderdate,
        receiptdate,
        datediff(day, orderdate, receiptdate) as fulfillment_days
    from sales
),

final as (
    select avg(fulfillment_days)
    from fulfillment_time
)

select * from final