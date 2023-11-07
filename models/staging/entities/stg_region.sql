select
    r_regionkey as regionkey,
    r_name as region_name

from {{ source('tpch_sf1', 'region') }}