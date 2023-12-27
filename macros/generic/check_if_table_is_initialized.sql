{% macro check_if_table_is_initialized(table_name) %}

SELECT ROW_COUNT
FROM ANALYTICS.INFORMATION_SCHEMA.TABLES
WHERE  table_schema = 'DBT_CORE_REUSABLE_DEMO'
AND    table_name   = {{table_name}}

{% endmacro %}