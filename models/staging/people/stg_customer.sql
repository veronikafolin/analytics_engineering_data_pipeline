select
    c_custkey as custkey,
    c_name as name,
    c_nationkey as nationkey,
    c_acctbal as acctbal,
    c_mktsegment as mktsegment

from {{ source('tpch_sf1', 'customer') }}