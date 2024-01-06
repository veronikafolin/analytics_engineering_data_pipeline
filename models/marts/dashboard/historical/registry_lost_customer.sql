with

orders as (
    select *
    from {{ref('registry_fct_orders')}}
    {{ apply_partition_date() }}
),

orders_filtered as (
    select *
    from orders
    {{ write_where_by_vars() }}
),

customers_beginning_of_period as (
    select distinct custkey
    from orders_filtered
    where orderdate <= '{{var("startPeriod")}}'
),

customers_end_of_period as (
    select distinct custkey
    from orders_filtered
    where orderdate >= '{{var("startPeriod")}}' and orderdate <= '{{var("endPeriod")}}'
),

lost_customers as (
    select * from customers_beginning_of_period
    except
    select * from customers_end_of_period
),

final as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        count(distinct custkey) as count_of_lost_customers
    from lost_customers
    join orders_filtered using(custkey)
    {{ write_groupBY_groupByColumns_by_vars() }}
)

select * from final