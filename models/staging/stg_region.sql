select
    r_regionkey,
    r_name

from {{ source('tpch_sf1', 'region') }}