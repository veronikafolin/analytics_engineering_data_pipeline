with supplier as (
    select * from {{ref('stg_supplier')}}
),
places as (
    select * from {{ref('int_places')}}
),
final as (
    select
        s.s_suppkey,
        s.s_name,
        p.pl_nation_name,
        p.pl_region_name,
        s.s_acctbal
    from supplier as s
    join places as p on p.pl_nationkey = s.s_nationkey
)

select * from final