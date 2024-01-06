{{config (
    cluster_by=['partition_date'],
    materialized='incremental',
    on_schema_change='append_new_columns'
)}}

with

part as (
    select *
    from {{ref('dim_part')}}
    where partition_date = (select MAX(partition_date) from {{ref('dim_part')}})
),

partsupp as (
    select *
    from {{ref('registry_stg_partsupp')}}
    where partition_date = (select MAX(partition_date) from {{ref('registry_stg_partsupp')}})
),

final as (
    select
        partsupp.partkey,
        partsupp.suppkey,
        partsupp.partsuppkey,
        partsupp.availqty,
        partsupp.supplycost,
        part.name as part_name,
        part.manufacturer as part_manufacturer,
        part.brand as part_brand,
        part.type as part_type,
        part.retailprice as part_retailprice,
        CURRENT_DATE() as partition_date
    from partsupp
    join part using(partkey)
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}