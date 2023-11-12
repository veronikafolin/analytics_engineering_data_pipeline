with

supplier as (
    select * from {{ref('dim_supplier')}}
),

final as (
    {{ compute_negative_values('supplier', 'supp_acctbal')}}
)

select * from final