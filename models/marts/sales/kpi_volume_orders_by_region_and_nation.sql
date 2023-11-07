with

final as (
    {{ compute_volume_orders(first='cust_nation_name', second='cust_region_name') }}
)

select * from final