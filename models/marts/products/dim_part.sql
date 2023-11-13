with

part as (
    select * from {{ref('stg_part')}}
),

final as (
    select
        partkey,
        name,
        manufacturer,
        brand,
        type,
        productsize,
        container,
        retailprice
    from part
)

select * from final