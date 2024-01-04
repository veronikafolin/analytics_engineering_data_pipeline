with

sales as (
    select * from {{ref('registry_fct_sales')}}
    {{ apply_partition_date() }}
),

filtered_sales as (
    select *
    from sales
    {{ write_where_by_vars() }}
),

delivery_time as (
    select
        *,
        datediff(day, shipdate, receiptdate) as delivery_days
    from filtered_sales
),

final as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        avg(delivery_days) as avg_delivery_days
    from delivery_time
    {{ write_groupBY_groupByColumns_by_vars() }}
)

select * from final