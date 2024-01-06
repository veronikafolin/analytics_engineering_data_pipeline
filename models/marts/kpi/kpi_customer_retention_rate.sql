with

orders as (
    select * from {{ref('fct_orders')}}
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

grouped_customers_beginning_of_period as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        count(distinct custkey) as customers_beginning_of_period
    from customers_beginning_of_period left join orders_filtered using (custkey)
    {{ write_groupBY_groupByColumns_by_vars() }}
),

customers_end_of_period as (
    select distinct custkey
    from orders_filtered
    where orderdate >= '{{var("startPeriod")}}' and orderdate <= '{{var("endPeriod")}}'
),

grouped_customers_end_of_period as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        count(distinct custkey) as customers_end_of_period
    from customers_end_of_period left join orders_filtered using (custkey)
    {{ write_groupBY_groupByColumns_by_vars() }}
),

acquired_customers as (
    select * from customers_end_of_period
    except
    select * from customers_beginning_of_period
),

grouped_acquired_customers as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        count(distinct custkey) as acquired_customers
    from acquired_customers left join orders_filtered using (custkey)
    {{ write_groupBY_groupByColumns_by_vars() }}
),

final as (
    select
        {{ write_select_groupByColumns_by_vars_from_table('grouped_acquired_customers') }}
        CAST(avg(grouped_customers_beginning_of_period.customers_beginning_of_period) AS INT) as customers_beginning_of_period,
        CAST(avg(grouped_customers_end_of_period.customers_end_of_period) AS INT) as customers_end_of_period,
        CAST(avg(grouped_acquired_customers.acquired_customers) AS INT) as acquired_customers,
        CAST(avg((customers_end_of_period - acquired_customers)/customers_beginning_of_period) * 100  AS DECIMAL(10,2))
        as customer_retention_rate
    from grouped_customers_beginning_of_period
    join grouped_customers_end_of_period
    join grouped_acquired_customers
    {{ write_groupBY_groupByColumns_by_vars_from_table('grouped_acquired_customers') }}
)

select * from final
