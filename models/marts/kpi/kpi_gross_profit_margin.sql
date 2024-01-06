with

sales as (
    select * from {{ref('fct_sales')}}
    {{ apply_partition_date() }}
),

inventory as (
    select * from {{ref('fct_inventory')}}
    {{ apply_partition_date() }}
),

filtered_sales as (
    select *
    from sales
    {{ write_where_by_vars() }}
),

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