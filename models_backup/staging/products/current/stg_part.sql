select
    p_partkey as partkey,
    p_name as name,
    p_mfgr as manufacturer,
    p_brand as brand,
    p_type as type,
    p_size as productsize,
    p_container as container,
    p_retailprice as retailprice

from {{ source('raw', 'part') }}