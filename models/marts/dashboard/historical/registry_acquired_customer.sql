{% set startPeriod = var("startPeriod") %}
{% set endPeriod = var("endPeriod") %}

with

sales as (
    select *
    from {{ref('registry_fct_sales')}}
    {{ apply_partition_date() }}
),

filtered_sales as (
    select *
    from sales
    {{ write_where_by_vars() }}
),

customers_beginning_of_period as (
    select distinct custkey
    from filtered_sales
    where orderdate <= '{{startPeriod}}'
),

customers_end_of_period as (
    select distinct custkey
    from filtered_sales
    where orderdate >= '{{startPeriod}}' and orderdate <= '{{endPeriod}}'
),

acquired_customers as (
    select * from customers_end_of_period
    except
    select * from customers_beginning_of_period
),

final as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        count(custkey) as count_of_acquired_customers
    from acquired_customers
    join filtered_sales using(custkey)
    {{ write_groupBY_groupByColumns_by_vars() }}
)

select * from final