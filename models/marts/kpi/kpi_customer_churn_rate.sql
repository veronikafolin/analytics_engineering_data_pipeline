{% set startPeriod = var("startPeriod") %}
{% set endPeriod = var("endPeriod") %}

with

orders as (
    select * from {{ref('fct_orders')}}
),

customers_beginning_of_period as (
    select distinct custkey
    from orders
    where orderdate <= '{{startPeriod}}'
),

customers_end_of_period as (
    select distinct custkey
    from orders
    where orderdate >= '{{startPeriod}}' and orderdate <= '{{endPeriod}}'
),

lost_customers as (
    select * from customers_beginning_of_period
    except
    select * from customers_end_of_period
),

final as (
    select
        ((select count(*) from lost_customers)/
        (select count(*) from customers_beginning_of_period)) * 100
        as customer_churn_rate
)

select * from final