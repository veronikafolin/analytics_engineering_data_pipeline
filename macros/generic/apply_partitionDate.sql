{% macro apply_partition_date() %}

{% set partitionByDate = var("partitionByDate") %}

    {% if partitionByDate != '' %}
        where partition_date = {{partitionByDate}}
    {% endif %}

{% endmacro %}
