with

sales as (
    select * from {{ref('fct_sales')}}
),

final as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        min(orderdate) as first_order_date,
        max(orderdate) as most_recent_order_date,
        count(distinct orderkey) as number_of_orders,
        count(lineitemkey) as number_of_sales,
        sum(quantity) as sum_of_quantity,
        sum(extendedprice) as total_revenue
    from sales
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
)

select * from final