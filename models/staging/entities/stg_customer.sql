select
    c_custkey as custkey,
    c_name as cust_name,
    c_nationkey as nationkey,
    c_acctbal as cust_acctbal,
    c_mktsegment as cust_mktsegment

from {{ source('tpch_sf1', 'customer') }}