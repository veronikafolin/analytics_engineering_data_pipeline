with

sales as (
    select * from {{ref('fct_sales')}}
),

inventory as (
    select * from {{ref('fct_inventory')}}
),

profit as (
    select
        sales.discounted_extended_price as revenue,
        (inventory.supplycost * sales.quantity) as cost_of_good_sold,
        (revenue - cost_of_good_sold) as profit
    from sales
    join inventory using(partsuppkey)
),

final as (
    select
        ((sum(revenue) - sum(cost_of_good_sold))/sum(revenue)) * 100 as gross_profit_margin
    from profit
)

select * from final