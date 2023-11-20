{% macro compute_cost_of_good_sold(supplycost, quantity) %}
    supplycost * quantity
{% endmacro %}