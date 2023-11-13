with

sales as (
    select * from {{ref('fct_sales')}}
),

returned_sales as (
    select *
    from sales
    where returnflag = 'R'
),

final as (
    select
        ((select count(*) from returned_sales) /
        (select count(*) from sales)) * 100
        as order_return_rate
)

select * from final