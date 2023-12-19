{{config (
    cluster_by=['partition_date'],
    materialized='incremental'
)}}

with

orders as (
    select *
    from {{ref('registry_stg_orders')}}
    {{ apply_partition_date() }}
),

customer as (
    select * from {{ref('dim_customer')}}
),

final as (
    select
        orders.orderkey,
        orders.orderstatus,
        orders.totalprice,
        orders.orderdate,
        orders.orderpriority,
        orders.clerk,
        orders.shippriority,
        customer.custkey,
        customer.cust_mktsegment,
        customer.cust_nation_name,
        customer.cust_region_name,
        CURRENT_DATE() as partition_date
    from orders
    join customer using(custkey)
)

select * from final

{% if is_incremental() %}

  partition_date > (select max(partition_date) from {{ this }})

{% endif %}