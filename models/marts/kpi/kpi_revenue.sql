with

orders as (
    select * from {{ref('fct_orders')}}
),

final as (

    select
        {{ write_select_groupByColumns_by_vars() }}
        sum(totalprice) as total_revenue
    from orders
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}

)

select * from final