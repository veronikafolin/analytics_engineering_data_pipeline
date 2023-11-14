with

sales_per_customer as (
    select * from {{ref('fct_sales_per_customer')}}
),

final as (
    select
        partsuppkey,
        sum(quantity) as sum_of_quantity
    from sales_per_customer
    where returnflag != 'R'
    group by partsuppkey
    order by sum_of_quantity DESC
    limit 10
)

select * from final