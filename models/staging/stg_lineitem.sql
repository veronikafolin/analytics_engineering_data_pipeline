select
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_partkey || '-' || l_suppkey as l_partsuppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_commitdate,
    l_receiptdate,
    l_shipinstruct,
    l_shipmode

from {{ source('tpch_sf1', 'lineitem') }}