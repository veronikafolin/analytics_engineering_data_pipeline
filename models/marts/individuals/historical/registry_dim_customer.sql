{{config (
    cluster_by=['partition_date'],
    materialized='incremental',
    on_schema_change='append_new_columns'
)}}

with

customer as (
    select *
    from {{ref('registry_stg_customer')}}
    where partition_date = (select MAX(partition_date) from {{ref('registry_stg_customer')}})
),

nation as (
    select *
    from {{ref('int_nation')}}
),

final as (
    select
        customer.custkey,
        customer.cust_name,
        customer.cust_acctbal,
        customer.cust_mktsegment,
        nation.nation_name as cust_nation_name,
        nation.region_name as cust_region_name,
        CURRENT_DATE() as partition_date
    from customer
    join nation using(nationkey)
)

select * from final

{% if is_incremental() %}

  where partition_date > (select max(partition_date) from {{ this }})

{% endif %}