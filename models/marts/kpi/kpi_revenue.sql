{% set groupBycolumns = var("groupBy") %}
{% set filters = var("filters") %}

with

orders as (
    select * from {{ref('stg_orders')}}
),

final as (

    select
        {% for col in groupBycolumns %}
        {{col}},
        {% endfor %}
        sum(totalprice) as total_revenue
    from orders
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