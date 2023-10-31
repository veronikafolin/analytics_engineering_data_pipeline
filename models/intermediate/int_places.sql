with nation as (
    select * from {{ref('stg_nation')}}
),
region as (
    select r_regionkey, r_name from {{ref('stg_region')}}
),
final as (
    select
        nation.n_nationkey,
        nation.n_name,
        region.r_name
    from
        nation
        left join region on nation.n_regionkey = region.r_regionkey
)
select * from final