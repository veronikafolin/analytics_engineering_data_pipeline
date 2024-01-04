{% macro write_select_groupByColumns_by_vars() %}

{% set groupBycolumns = var("groupBy") %}

    {% for col in groupBycolumns %}
        {{col}},
    {% endfor %}

{% endmacro %}

{% macro write_groupBY_groupByColumns_by_vars() %}

{% set groupBycolumns = var("groupBy") %}

    {% if groupBycolumns|length > 0 %}
        group by
            {% for col in groupBycolumns %}
            {{col}}{% if not loop.last %},{% endif %}
            {% endfor %}
    {% endif %}

{% endmacro %}

{% macro write_groupByColumns_by_vars() %}

{% set groupBycolumns = var("groupBy") %}

    {% if groupBycolumns|length > 0 %}
        {% for col in groupBycolumns %}
        , {{col}}
        {% endfor %}
    {% endif %}

{% endmacro %}