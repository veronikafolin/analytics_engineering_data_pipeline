with

sales as (
    select * from {{ref('fct_sales')}}
),

final as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        count(distinct orderkey) as number_of_orders,
        avg(datediff(day, shipdate, receiptdate)) as delivery_days,
        avg(datediff(day, orderdate, receiptdate)) as fulfillment_days
    from sales
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
)

select * from final