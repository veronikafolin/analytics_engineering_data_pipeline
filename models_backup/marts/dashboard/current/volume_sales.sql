with

sales as (
    select * from {{ref('fct_sales')}}
),

inventory as (
    select * from {{ref('fct_inventory')}}
),

final as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        min(orderdate) as first_order_date,
        max(orderdate) as most_recent_order_date,
        count(lineitemkey) as number_of_sales,
        sum(quantity) as sum_of_quantity,
        sum(extendedprice) as sum_extendedprice,
        sum(discounted_extended_price) as sum_discounted_extended_price,
        sum( {{ compute_cost_of_good_sold(supplycost, quantity) }} ) as sum_cost_of_good_sold,
        sum( {{ compute_profit(discounted_extended_price, supplycost, quantity) }} ) as sum_profit
    from sales
    join inventory using(partsuppkey)
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
)

select * from final