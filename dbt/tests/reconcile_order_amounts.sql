with item_totals as (
    select
        order_id,
        cast(sum(line_amount) as decimal(18, 2)) as item_total
    from {{ ref('fact_order_item') }}
    group by order_id
)

select
    orders.order_id,
    orders.order_total as expected_amount,
    coalesce(item_totals.item_total, 0) as actual_amount,
    coalesce(item_totals.item_total, 0) - orders.order_total as amount_difference
from {{ ref('fact_order') }} as orders
left join item_totals
    on orders.order_id = item_totals.order_id
where abs(coalesce(item_totals.item_total, 0) - orders.order_total) > 0.01
