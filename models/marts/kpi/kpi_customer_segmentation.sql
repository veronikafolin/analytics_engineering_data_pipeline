{% set groupBycolumns = var("groupBy") %}
{% set filters = var("filters") %}

with

customers as (
    select * from {{ref('dim_customer')}}
),

final as (
    select
    {% for col in groupBycolumns %}
        {{col}},
    {% endfor %}
        count(custkey) as number_of_customers
    from customers
    {% for filter in filters %}
    where {{filter["field"]}} = '{{filter["value"]}}'
    {% endfor %}
    {% if groupBycolumns|length > 0 %}
        group by
            {% for col in groupBycolumns %}
            {{col}}{% if not loop.last %},{% endif %}
            {% endfor %}
    {% endif %}
)

select * from final