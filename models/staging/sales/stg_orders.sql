select
    o_orderkey as orderkey,
    o_custkey as custkey,
    o_orderstatus as orderstatus,
    o_totalprice as totalprice,
    o_orderdate as orderdate,
    o_orderpriority as orderpriority,
    o_clerk as clerk,
    o_shippriority as shippriority,
    DATE(dbt_updated_at) as updated_at,
    DATE(dbt_valid_from) as valid_from,
    DATE(dbt_valid_to) as valid_to

from {{ref('orders_snapshot')}}