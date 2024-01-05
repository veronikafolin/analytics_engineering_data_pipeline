with

sales as (
    select * from {{ref('registry_fct_sales')}}
    {{ apply_partition_date() }}
),

inventory as (
    select * from {{ref('registry_fct_inventory')}}
    {{ apply_partition_date() }}
),

filtered_sales as (
    select *
    from sales
    {{ write_where_by_vars() }}
),

--profit as (
--    select
--        filtered_sales.discounted_extended_price as revenue,
--        (inventory.supplycost * sales.quantity) as cost_of_good_sold,
--        (revenue - cost_of_good_sold) as profit
--    from filtered_sales
--    join inventory using(partsuppkey)
--),
--
--final as (
--    select
--        ((sum(revenue) - sum(cost_of_good_sold))/sum(revenue)) * 100 as gross_profit_margin
--    from profit
--)

final as (
    select
        {{ write_select_groupByColumns_by_vars_from_table('filtered_sales') }}
        sum(filtered_sales.discounted_extended_price) as revenue,
        sum(inventory.supplycost * filtered_sales.quantity) as cost_of_good_sold,
        sum(filtered_sales.discounted_extended_price - (inventory.supplycost * filtered_sales.quantity)) as profit
    from filtered_sales join inventory using(partsuppkey)
    {{ write_groupBY_groupByColumns_by_vars_from_table('filtered_sales') }}
)

select * from final