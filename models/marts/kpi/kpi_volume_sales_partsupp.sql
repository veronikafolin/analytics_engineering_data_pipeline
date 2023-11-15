with

sales as (
    select * from {{ref('fct_sales')}}
),

final as (
    select
        partsuppkey,
        min(orderdate) as first_order_date,
        max(orderdate) as most_recent_order_date,
        count(distinct orderkey) as number_of_orders,
        sum(quantity) as sum_of_quantity,
        sum(extendedprice) as total_revenue
    from sales
    group by partsuppkey
)

select * from final