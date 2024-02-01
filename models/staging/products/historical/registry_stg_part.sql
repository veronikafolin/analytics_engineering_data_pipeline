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
        IFF(new.p_partkey is null, old.partkey, new.p_partkey) as partkey,
        IFF(new.p_partkey is null, old.name, new.p_name) as name,
        IFF(new.p_partkey is null, old.manufacturer, new.p_mfgr) as manufacturer,
        IFF(new.p_partkey is null, old.brand, new.p_brand) as brand,
        IFF(new.p_partkey is null, old.type, new.p_type) as type,
        IFF(new.p_partkey is null, old.productsize, new.p_size) as productsize,
        IFF(new.p_partkey is null, old.container, new.p_container) as container,
        IFF(new.p_partkey is null, old.retailprice, new.p_retailprice) as retailprice,
        CURRENT_DATE() as partition_date
    from last_snapshot as new FULL OUTER JOIN previous_state_of_registry as old ON new.p_partkey = old.partkey
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}