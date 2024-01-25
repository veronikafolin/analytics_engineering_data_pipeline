select
    s_suppkey as suppkey,
    s_name as supp_name,
    s_nationkey as nationkey,
    s_acctbal as supp_acctbal

from {{ source('raw', 'supplier') }}