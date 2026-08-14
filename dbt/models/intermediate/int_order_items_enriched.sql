select
    items.order_item_id,
    items.order_id,
    items.line_number,
    orders.customer_id,
    items.product_id,
    products.sku,
    products.product_name,
    products.category,
    orders.order_status,
    orders.ordered_at,
    cast(orders.ordered_at as date) as order_date,
    items.quantity,
    items.unit_price,
    cast(items.quantity * items.unit_price as decimal(18, 2)) as line_amount,
    items.source_updated_at,
    items.source_batch_id
from {{ ref('stg_order_items') }} as items
left join {{ ref('stg_orders') }} as orders
    on items.order_id = orders.order_id
left join {{ ref('stg_products') }} as products
    on items.product_id = products.product_id
