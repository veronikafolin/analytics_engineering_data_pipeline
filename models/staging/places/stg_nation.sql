select
    n_nationkey as nationkey,
    n_name as nation_name,
    n_regionkey as regionkey

from {{ source('raw', 'nation') }}