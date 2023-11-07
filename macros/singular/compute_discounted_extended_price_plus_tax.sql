{% macro compute_discounted_extended_price_plus_tax(extendedprice, discount, tax) %}
    extendedprice * (1 - discount) + (1 + tax)
{% endmacro %}