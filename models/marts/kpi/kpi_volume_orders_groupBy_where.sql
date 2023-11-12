with

final as (
    {{ compute_volume_orders_groupBy_where() }}
)

select * from final