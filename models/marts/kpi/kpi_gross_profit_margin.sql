with

sales as (
    select * from {{ref('registry_fct_sales')}}
    {{ apply_partition_date() }}
),

filtered_sales as (
    select *
    from sales
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
),

inventory as (
    select * from {{ref('registry_fct_inventory')}}
    {{ apply_partition_date() }}
),

filtered_inventory as (
    select *
    from inventory
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
),

profit as (
    select
        sales.discounted_extended_price as revenue,
        (inventory.supplycost * sales.quantity) as cost_of_good_sold,
        (revenue - cost_of_good_sold) as profit
    from filtered_sales
    join filtered_inventory using(partsuppkey)
),

final as (
    select
        ((sum(revenue) - sum(cost_of_good_sold))/sum(revenue)) * 100 as gross_profit_margin
    from profit
)

select * from final