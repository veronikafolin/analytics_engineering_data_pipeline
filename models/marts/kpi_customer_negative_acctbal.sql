with customer as (
    select * from {{ref('stg_customer')}}
),
final as (
    select
        c_custkey,
        c_acctbal
    from customer
    where c_acctbal < 0
)
select * from final