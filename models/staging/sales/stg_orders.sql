select
    o_orderkey as orderkey,
    o_custkey as custkey,
    o_orderstatus as orderstatus,
    o_totalprice as totalprice,
    o_orderdate as orderdate,
    o_orderpriority as orderpriority,
    o_clerk as clerk,
    o_shippriority as shippriority

from {{ source('tpch_sf1', 'orders') }}