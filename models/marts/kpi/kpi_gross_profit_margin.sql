with

sales as (
    select * from {{ref('fct_sales')}}
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
        CAST(sum(net_revenue) AS INT) as net_revenue,
        CAST(sum(cost_of_good_sold) AS INT) as cost_of_good_sold,
        CAST(sum(profit) AS INT) as profit
    from filtered_sales
    {{ write_groupBY_groupByColumns_by_vars_from_table('filtered_sales') }}
)

select * from final