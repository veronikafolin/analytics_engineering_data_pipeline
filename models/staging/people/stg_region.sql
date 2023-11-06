select
    r_regionkey as regionkey,
    r_name as name

from {{ source('tpch_sf1', 'region') }}