with

nation as (
    select * from {{ref('stg_nation')}}
),

region as (
    select * from {{ref('stg_region')}}
),

final as (
    select *
    from nation
    join region using(regionkey)
)

select * from final