{{
    config(
        cluster_by=['partition_date'],
        materialized='incremental',
        on_schema_change='append_new_columns'
    )
}}

with

last_snapshot as (
    select *
    from {{ref('snapshot_part')}}
    where DATE(DBT_VALID_FROM) = (select MAX(DATE(DBT_VALID_FROM)) from {{ref('snapshot_part')}})
),

previous_state_of_registry as (
    select *
    from {{this}}
    where partition_date = (select MAX(partition_date) from {{this}})
),

final as (
    select
        COALESCE(new.p_partkey, old.partkey) as partkey,
        COALESCE(new.p_name, old.name) as name,
        COALESCE(new.p_mfgr, old.manufacturer) as manufacturer,
        COALESCE(new.p_brand, old.brand) as brand,
        COALESCE(new.p_type, old.type) as type,
        COALESCE(new.p_size, old.productsize) as productsize,
        COALESCE(new.p_container, old.container) as container,
        COALESCE(new.p_retailprice, old.retailprice) as retailprice,
        CURRENT_DATE() as partition_date
    from last_snapshot as new FULL OUTER JOIN previous_state_of_registry as old ON new.p_partkey = old.partkey
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}