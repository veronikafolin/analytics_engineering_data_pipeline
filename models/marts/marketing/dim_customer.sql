with

customer as (
    select * from {{ref('stg_customer')}}
),

nation as (
    select * from {{ref('int_nation')}}
),

final as (
    select
        custkey,
        cust_name,
        cust_acctbal,
        cust_mktsegment,
        nation_name as cust_nation_name,
        region_name as cust_region_name
    from customer
    join nation using(nationkey)
)

select * from final