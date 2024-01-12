with

sales as (
    select *
    from {{ref('fct_sales')}}
    {{ apply_partition_date() }}
),

final as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        min(orderdate) as first_order_date,
        max(orderdate) as most_recent_order_date,
        count(lineitemkey) as number_of_sales,
        count(distinct orderkey) as number_of_orders,
        count(distinct custkey) as number_of_customers,
        sum(quantity) as sum_of_quantity,
        CAST(sum(extendedprice) AS INT) as sum_extendedprice,
        CAST(sum(net_revenue) AS INT) as sum_net_revenue,
        CAST(sum(gross_revenue) AS INT) as sum_gross_revenue,
        CAST(sum(cost_of_good_sold) AS INT) as sum_cost_of_good_sold,
        CAST(sum(profit) AS INT) as sum_profit
    from sales
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
)

select * from final