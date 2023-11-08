{% macro compute_volume_orders() %}

select
    {% for key, value in kwargs.items() %}
    {{value}},
    {% endfor %}
    min(orderdate) as first_order_date,
    max(orderdate) as most_recent_order_date,
    count(orderkey) as number_of_orders,
    sum(totalprice) as sum_total_price
from
    {{ref('fct_orders')}}
group by
    {% for key, value in kwargs.items() %}
    {{value}}{% if not loop.last %},{% endif %}
    {% endfor %}

{% endmacro %}