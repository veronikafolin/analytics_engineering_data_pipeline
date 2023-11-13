{% macro compute_average_order_value() %}

{% set groupBycolumns = var("groupBy") %}
{% set filters = var("filters") %}

select
    {% for col in groupBycolumns %}
    {{col}},
    {% endfor %}
    sum(totalprice)/count(orderkey) as average_order_value
from
    {{ref('fct_orders')}}
{% for filter in filters %}
where {{filter["field"]}} = '{{filter["value"]}}'
{% endfor %}
{% if groupBycolumns|length > 0 %}
    group by
        {% for col in groupBycolumns %}
        {{col}}{% if not loop.last %},{% endif %}
        {% endfor %}
{% endif %}

{% endmacro %}