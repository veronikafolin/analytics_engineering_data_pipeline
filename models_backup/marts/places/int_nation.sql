with

nation as (
    select * from {{ref('stg_nation')}}
),

region as (
    select * from {{ref('stg_region')}}
),

final as (
    select
        nation.nationkey,
        nation.nation_name,
        region.region_name
    from nation
    join region using(regionkey)
)

select * from final