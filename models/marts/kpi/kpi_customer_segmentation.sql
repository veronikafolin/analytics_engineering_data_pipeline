with

customers as (
    select * from {{ref('dim_customer')}}
),

final as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        count(custkey) as number_of_customers
    from customers
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
)

select * from final