{% macro compute_volume_orders_groupBy_where() %}

{% set groupBycolumns = var("groupBy") %}

{% set filters = [var("filter")] %}

select
    {% for col in groupBycolumns %}
    {{col}},
    {% endfor %}
    min(orderdate) as first_order_date,
    max(orderdate) as most_recent_order_date,
    count(orderkey) as number_of_orders,
    sum(totalprice) as sum_total_price
from
    {{ref('fct_orders')}}
{% for filter in filters %}
where {{filter["field"]}} = '{{filter["value"]}}'
{% endfor %}
group by
    {% for col in groupBycolumns %}
    {{col}}{% if not loop.last %},{% endif %}
    {% endfor %}

{% endmacro %}