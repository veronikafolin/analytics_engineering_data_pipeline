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

fulfillment_time as (
    select
        *,
        datediff(day, orderdate, receiptdate) as fulfillment_days
    from filtered_sales
),

final as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        avg(fulfillment_days) as avg_fulfillment_days
    from fulfillment_time
    {{ write_groupBY_groupByColumns_by_vars() }}
)

select * from final