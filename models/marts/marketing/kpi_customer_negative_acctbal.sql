with

customer as (
    select * from {{ref('dim_customer')}}
),

final as (
    select *
    from customer
    where cust_acctbal < 0
)

select * from final