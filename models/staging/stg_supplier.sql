select
    s_suppkey,
    s_name,
    s_nationkey,
    s_acctbal

from {{ source('tpch_sf1', 'supplier') }}