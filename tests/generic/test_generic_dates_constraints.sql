{% test test_generic_dates_constraints(model, column_name, next_date) %}
select *
from {{ model }}
where {{column_name}} > {{next_date}}
{% endtest %}