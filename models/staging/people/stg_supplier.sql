select
    s_suppkey as suppkey,
    s_name as name,
    s_nationkey as nationkey,
    s_acctbal as acctbal

from {{ source('tpch_sf1', 'supplier') }}