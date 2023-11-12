with

part as (
    select * from {{ref('stg_part')}}
),

partsupp as (
    select * from {{ref('stg_partsupp')}}
),

final as (
    select
        partsupp.partkey,
        partsupp.suppkey,
        partsupp.availqty,
        partsupp.supplycost,
        part.name,
        part.manufacturer,
        part.brand,
        part.type,
        part.retailprice
    from partsupp
    join part using(partkey)
)

select * from final