with

customers as (
    select * from {{ref('registry_dim_customer')}}
    {{ apply_partition_date() }}
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