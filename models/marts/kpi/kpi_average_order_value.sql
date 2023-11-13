with

final as (
    {{ compute_average_order_value() }}
)

select * from final