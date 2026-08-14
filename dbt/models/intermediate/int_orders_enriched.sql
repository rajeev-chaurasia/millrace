with item_totals as (
    select
        order_id,
        cast(sum(quantity * unit_price) as decimal(18, 2)) as calculated_item_total,
        sum(quantity) as item_quantity,
        count(*) as item_count
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    orders.order_id,
    orders.customer_id,
    customers.first_name,
    customers.last_name,
    customers.email,
    orders.order_status,
    orders.ordered_at,
    cast(orders.ordered_at as date) as order_date,
    coalesce(item_totals.calculated_item_total, 0) as order_total,
    coalesce(item_totals.item_quantity, 0) as item_quantity,
    coalesce(item_totals.item_count, 0) as item_count,
    orders.source_updated_at,
    orders.source_batch_id
from {{ ref('stg_orders') }} as orders
left join {{ ref('stg_customers') }} as customers
    on orders.customer_id = customers.customer_id
left join item_totals
    on orders.order_id = item_totals.order_id
