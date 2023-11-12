select
    ps_partkey as partkey,
    ps_suppkey as suppkey,
    {{ dbt_utils.generate_surrogate_key(['partkey', 'suppkey'])}} as partsuppkey,
    ps_availqty as availqty,
    ps_supplycost as supplycost

from {{ source('tpch_sf1', 'partsupp') }}