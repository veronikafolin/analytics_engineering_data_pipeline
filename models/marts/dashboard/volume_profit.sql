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
        avg(discounted_extended_price) as revenue,
        avg( {{ compute_cost_of_good_sold(supplycost, quantity) }} ) as cost_of_good_sold,
        avg( {{ compute_profit(discounted_extended_price, supplycost, quantity) }} ) as profit
    from sales
    join inventory using(partsuppkey)
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
)

select * from final