{{config (
    cluster_by=['partition_date'],
    materialized='incremental',
    on_schema_change='append_new_columns'
)}}

with

supplier as (
    select *
    from {{ref('registry_stg_supplier')}}
    where partition_date = (select MAX(partition_date) from {{ref('registry_stg_supplier')}})
),

nation as (
    select * from {{ref('int_nation')}}
),

final as (
    select
        supplier.suppkey,
        supplier.supp_name,
        supplier.supp_acctbal,
        nation.nation_name as supp_nation_name,
        nation.region_name as supp_region_name,
        CURRENT_DATE() as partition_date
    from supplier
    join nation using(nationkey)
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}