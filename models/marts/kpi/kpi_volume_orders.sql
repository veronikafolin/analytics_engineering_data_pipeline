with

orders as (
    select * from {{ref('fct_orders')}}
),

final as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        min(orderdate) as first_order_date,
        max(orderdate) as most_recent_order_date,
        count(orderkey) as number_of_orders,
        count(distinct custkey) as number_of_customers,
        sum(totalprice) as total_revenue
    from orders
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
)

select * from final