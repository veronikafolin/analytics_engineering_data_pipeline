{% macro write_where_by_vars() %}

{% set filters = var("filters") %}

    {% if filters|length > 0 %}
        where
        {% for filter in filters %}
    --        where {{filter["field"]}} = '{{filter["value"]}}'
            {{filter}}{% if not loop.last %} and {% endif %}
        {% endfor %}
    {% endif %}

{% endmacro %}
