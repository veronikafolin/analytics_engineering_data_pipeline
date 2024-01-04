with

orders as (
    select * from {{ref('registry_fct_orders')}}
    {{ apply_partition_date() }}
),

final as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        avg(totalprice) as avg_revenue
    from orders
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
)

select * from final