select
    c_custkey as custkey,
    c_name as cust_name,
    c_nationkey as nationkey,
    c_acctbal as cust_acctbal,
    c_mktsegment as cust_mktsegment,
    DATE(dbt_valid_from) as valid_from,
    DATE(dbt_valid_to) as valid_to

from {{ref('customer_snapshot')}}