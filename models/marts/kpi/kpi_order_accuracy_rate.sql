with

orders as (
    select * from {{ref('registry_fct_orders')}}
    {{ apply_partition_date() }}
),

filtered_orders as (
    select *
    from orders
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
),

fulfilled_orders as (
    select orderkey
    from filtered_orders
    where orderstatus = 'F'
),

final as (
    select
    ((select count(orderkey) from fulfilled_orders) /
    (select count(orderkey) from orders)) * 100
    as order_accuracy_rate
)

select * from final