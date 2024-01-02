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

final as (
    select
        partsuppkey,
        sum(quantity) as sum_of_quantity
    from filtered_sales
    where returnflag != 'R'
    group by partsuppkey
    order by sum_of_quantity DESC
    limit 10
)

select * from final