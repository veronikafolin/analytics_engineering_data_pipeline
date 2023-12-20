{{config (
    cluster_by=['partition_date'],
    materialized='incremental'
)}}

with

customer as (
    select *
    from {{ref('registry_stg_customer')}}
    {{ apply_partition_date() }}
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

  partition_date > (select max(partition_date) from {{ this }})

{% endif %}