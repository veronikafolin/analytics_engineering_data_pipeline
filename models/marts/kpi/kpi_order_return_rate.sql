with

sales as (
    select * from {{ref('registry_fct_sales')}}
    {{ apply_partition_date() }}
),

filtered_sales as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        count(*) as number_of_sales
    from sales
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
),

returned_sales as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        count(*) as number_of_sales
    from filtered_sales
    where returnflag = 'R'
    {{ write_groupBY_groupByColumns_by_vars() }}
),

--final as (
--    select
--        ((select count(*) from returned_sales) /
--        (select count(*) from filtered_sales)) * 100
--        as order_return_rate
--)

final as (
    select
        ((select number_of_sales from returned_sales) /
        (select number_of_sales from filtered_sales)) * 100
        as order_return_rate
)

select * from final