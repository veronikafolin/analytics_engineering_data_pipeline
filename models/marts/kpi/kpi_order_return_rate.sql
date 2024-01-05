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

grouped_filtered_sales as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        count(*) as number_of_sales
    from filtered_sales
    {{ write_groupBY_groupByColumns_by_vars() }}
),

returned_sales as (
    select *
    from filtered_sales
    where returnflag = 'R'
),

grouped_returned_sales as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        count(*) as number_of_returned_sales
    from returned_sales
    {{ write_groupBY_groupByColumns_by_vars() }}
),

final as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        grouped_filtered_sales.number_of_sales,
        grouped_returned_sales.number_of_returned_sales,
        (number_of_returned_sales/number_of_sales)*100 as order_return_rate
    from grouped_filtered_sales LEFT JOIN grouped_returned_sales
    {{ write_groupBY_groupByColumns_by_vars() }}
)

--final as (
--    select
--        ((select count(*) from returned_sales) /
--        (select count(*) from filtered_sales)) * 100
--        as order_return_rate
--)
select * from final