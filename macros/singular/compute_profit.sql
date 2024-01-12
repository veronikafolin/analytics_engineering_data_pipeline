{% macro compute_profit(net_revenue, supplycost, quantity) %}
    net_revenue - (supplycost * quantity)
{% endmacro %}