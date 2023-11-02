with lineitem as (
    select * from {{ref('stg_lineitem')}}
),
orders as (
    select * from {{ref('stg_orders')}}
),
final as (
    select
        lineitem.l_suppkey,
        lineitem.l_partkey,
        min(orders.o_orderdate) as first_order_date,
        max(orders.o_orderdate) as most_recent_order_date,
        count(orders.o_orderkey) as number_of_orders,
        sum(lineitem.l_quantity) as sum_of_quantity,
        sum(lineitem.l_extendedprice) as total_revenue
    from lineitem
    join orders on orders.o_orderkey = lineitem.l_orderkey
    group by 1, 2
    order by 1
)
select * from final