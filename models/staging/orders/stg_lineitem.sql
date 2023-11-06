select
    l_orderkey as orderkey,
    l_partkey as partkey,
    l_suppkey as suppkey,
    l_partkey || '-' || l_suppkey as partsuppkey,
    l_linenumber as linenumber,
    CAST(l_quantity AS int) as quantity,
    l_extendedprice as extendedprice,
    l_discount as discount,
    l_tax as tax,
    l_returnflag as returnflag,
    l_linestatus as linestatus,
    l_shipdate as shipdate,
    l_commitdate as commitdate,
    l_receiptdate as receiptdate,
    l_shipinstruct as shipinstruct,
    l_shipmode as shipmode

from {{ source('tpch_sf1', 'lineitem') }}