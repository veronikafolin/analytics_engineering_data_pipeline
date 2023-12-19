with

customer as (
    select * from {{ref('stg_customer')}}
    where valid_to is null
),

nation as (
    select * from {{ref('int_nation')}}
),

final as (
    select
        customer.custkey,
        customer.cust_name,
        customer.cust_acctbal,
        customer.cust_mktsegment,
        nation.nation_name as cust_nation_name,
        nation.region_name as cust_region_name
    from customer
    join nation using(nationkey)
)

select * from final