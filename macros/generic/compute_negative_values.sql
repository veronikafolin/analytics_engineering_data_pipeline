{% macro compute_negative_values(model, column_name) %}

select *
from {{model}}
where {{column_name}} < 0

{% endmacro %}