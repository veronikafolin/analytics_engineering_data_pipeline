with

final as (
    {{ compute_volume_orders() }}
)

select * from final