select
    r_regionkey as regionkey,
    r_name as region_name

from {{ source('raw', 'region') }}