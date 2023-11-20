{% macro compute_profit(discounted_extended_price, supplycost, quantity) %}
    discounted_extended_price - (supplycost * quantity)
{% endmacro %}