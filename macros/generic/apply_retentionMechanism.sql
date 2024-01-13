{% macro apply_retention_mechanism(retentionDays) %}

    {% set partitionByDate = var("partitionByDate") %}

    where partition_date = DATEADD(DAY, -{{retentionDays}}, {{partitionByDate}})

{% endmacro %}
