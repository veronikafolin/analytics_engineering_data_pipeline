with

orders as (
    select * from {{ref('registry_fct_orders')}}
    {{ apply_partition_date() }}
),

filtered_orders as (
    select *
    from orders
    {{ write_where_by_vars() }}
),

grouped_filtered_orders as (
    select
       {{ write_select_groupByColumns_by_vars() }}
       count(*) as number_of_orders
    from filtered_orders
    {{ write_groupBY_groupByColumns_by_vars() }}

),

fulfilled_orders as (
    select *
    from filtered_orders
    where orderstatus = 'F'
),

grouped_fulfilled_orders as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        count(*) as number_of_fulfilled_orders
    from fulfilled_orders
    {{ write_groupBY_groupByColumns_by_vars() }}
),

final as (
    select
        {{ write_select_groupByColumns_by_vars_from_table('grouped_filtered_orders') }}
        CAST(avg(grouped_filtered_orders.number_of_orders) AS INT) as number_of_orders,
        CAST(avg(grouped_fulfilled_orders.number_of_fulfilled_orders) AS INT) as number_of_fulfilled_orders,
        CAST(avg((number_of_fulfilled_orders/number_of_orders)*100) AS DECIMAL(10,2)) as order_accuracy_rate
    from grouped_filtered_orders INNER JOIN grouped_fulfilled_orders
    {{ write_groupBY_groupByColumns_by_vars_from_table('grouped_filtered_orders') }}
)

select * from final