select
    ps_partkey,
    ps_suppkey,
    ps_partkey || '-' || ps_suppkey as ps_surrogate_key,
    ps_availqty,
    ps_supplycost

from {{ source('tpch_sf1', 'partsupp') }}