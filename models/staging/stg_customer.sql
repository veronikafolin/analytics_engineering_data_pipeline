select
    c_custkey,
    c_name,
    c_nationkey,
    c_acctbal,
    c_mktsegment

from {{ source('tpch_sf1', 'customer') }}