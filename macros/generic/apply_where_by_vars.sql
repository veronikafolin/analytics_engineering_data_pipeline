{% macro write_where_by_vars() %}

{% set filters = var("filters") %}

    {% for filter in filters %}
        where {{filter["field"]}} = '{{filter["value"]}}'
    {% endfor %}

{% endmacro %}
