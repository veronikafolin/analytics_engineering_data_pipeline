{% macro compute_discounted_extended_price_plus_tax(extended_price, discount, tax) %}
    extended_price * (1 - discount) + (1 + tax)
{% endmacro %}