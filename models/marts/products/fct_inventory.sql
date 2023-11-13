with

part as (
    select * from {{ref('dim_part')}}
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
        part.name as part_name,
        part.manufacturer as part_manufacturer,
        part.brand as part_brand,
        part.type as part_type,
        part.retailprice as part_retailprice
    from partsupp
    join part using(partkey)
)

select * from final