{% macro compute_discounted_extended_price(extendedprice, discount) %}
    extendedprice * (1 - discount)
{% endmacro %}