with

supplier as (
    select * from {{ref('stg_supplier')}}
),

nation as (
    select * from {{ref('int_nation')}}
),

final as (
    select
        supplier.suppkey,
        supplier.supp_name,
        supplier.supp_acctbal,
        nation.nation_name as supp_nation_name,
        nation.region_name as supp_region_name
    from supplier
    join nation using(nationkey)
)

select * from final