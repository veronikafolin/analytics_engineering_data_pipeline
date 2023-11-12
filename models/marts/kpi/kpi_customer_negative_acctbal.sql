with

customer as (
    select * from {{ref('dim_customer')}}
),

final as (
    {{ compute_negative_values('customer', 'cust_acctbal')}}
)

select * from final