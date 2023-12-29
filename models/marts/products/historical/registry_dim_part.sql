{{config (
    cluster_by=['partition_date'],
    materialized='incremental',
    on_schema_change='append_new_columns'
)}}

with

part as (
    select *
    from {{ref('registry_stg_part')}}
    {{ apply_partition_date() }}
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
        retailprice,
        CURRENT_DATE() as partition_date
    from part
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}