with

sales as (
    select * from {{ref('registry_fct_sales')}}
    {{ apply_partition_date() }}
),

filtered_sales as (
    select *
    from sales
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
),

returned_sales as (
    select *
    from filtered_sales
    where returnflag = 'R'
),

final as (
    select
        ((select count(*) from returned_sales) /
        (select count(*) from sales)) * 100
        as order_return_rate
)

select * from final