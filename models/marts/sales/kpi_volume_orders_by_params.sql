with

final as (
    {{ compute_volume_orders_by_params(first='cust_nation_name') }}
)

select * from final