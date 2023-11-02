{% macro compute_discounted_extended_price(extended_price, discount) %}
    extended_price * (1 - discount)
{% endmacro %}