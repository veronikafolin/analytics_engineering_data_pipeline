with

sales as (
    select * from {{ref('registry_fct_sales')}}
    {{ apply_partition_date() }}
),

filtered_sales as (
    select *
    from sales
),

delivery_time as (
    select
        shipdate,
        receiptdate,
        datediff(day, shipdate, receiptdate) as delivery_days
    from filtered_sales
),

final as (
    select avg(delivery_days) as avg_delivery_days
    from delivery_time
)

select * from final