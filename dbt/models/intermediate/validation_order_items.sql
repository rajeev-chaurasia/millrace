{{ config(tags=['candidate']) }}

select
    order_id,
    line_number,
    product_id,
    quantity,
    unit_price,
    source_updated_at as updated_at,
    source_batch_id as batch_id
from {{ ref('stg_order_items') }}
