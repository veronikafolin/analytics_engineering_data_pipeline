with lineitem as (
    select * from {{ref('stg_lineitem')}}
),

orders as (
    select * from {{ref('dim_orders')}}
),

part as (
    select * from {{ref('stg_part')}}
),

supplier as (
    select * from {{ref('dim_supplier')}}
),

final as (
    select
        lineitem.l_orderkey,
        lineitem.l_linenumber,
        orders.c_name,
        part.p_name,
        part.p_mfgr,
        part.p_brand,
        part.p_type,
        part.p_size,
        part.p_container,
        part.p_retailprice,
        supplier.s_name,
        supplier.pl_nation_name,
        supplier.pl_region_name,
        lineitem.l_quantity,
        lineitem.l_extendedprice as extended_price,
        lineitem.l_discount as discount,
        {{ compute_discounted_extended_price('extended_price', 'discount') }} as discounted_extended_price,
        lineitem.l_tax as tax,
        {{ compute_discounted_extended_price_plus_tax('extended_price', 'discount', 'tax') }} as discounted_extended_price_plus_tax,
        lineitem.l_returnflag,
        lineitem.l_linestatus,
        lineitem.l_shipdate,
        lineitem.l_commitdate,
        lineitem.l_receiptdate,
        lineitem.l_shipinstruct,
        lineitem.l_shipmode
    from lineitem
    join orders on orders.o_orderkey = lineitem.l_orderkey
    join part on part.p_partkey = lineitem.l_partkey
    join supplier on supplier.s_suppkey = lineitem.l_suppkey
    order by 1
)

select * from final

