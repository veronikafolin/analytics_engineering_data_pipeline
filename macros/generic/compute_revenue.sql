{% macro compute_revenue() %}

{% set groupBycolumns = var("groupBy") %}
{% set filters = var("filters") %}

select
    {% for col in groupBycolumns %}
    {{col}},
    {% endfor %}
    sum(totalprice) as total_revenue
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