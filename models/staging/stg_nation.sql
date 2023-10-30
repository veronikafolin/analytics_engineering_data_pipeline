select
    n_nationkey,
    n_name,
    n_regionkey

from {{ source('tpch_sf1', 'nation') }}