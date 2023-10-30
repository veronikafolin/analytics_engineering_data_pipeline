select
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost

from {{ source('tpch_sf1', 'partsupp') }}