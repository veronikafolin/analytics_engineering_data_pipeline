with

part as (
    select * from {{ref('stg_part')}}
),

partsupp as (
    select * from {{ref('stg_partsupp')}}
),

final as (
    select *
    from partsupp
    join part using(partkey)
)

select * from final