with

supplier as (
    select * from {{ref('stg_supplier')}}
),

nation as (
    select * from {{ref('int_nation')}}
),

final as (
    select
        suppkey,
        supp_name,
        supp_acctbal,
        nation_name as supp_nation_name,
        region_name as supp_region_name
    from supplier
    join nation using(nationkey)
)

select * from final