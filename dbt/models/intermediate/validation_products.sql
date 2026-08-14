{{ config(tags=['candidate']) }}

select
    product_id,
    sku,
    product_name as name,
    category,
    unit_price,
    is_active as "active",
    source_updated_at as updated_at,
    source_batch_id as batch_id
from {{ ref('stg_products') }}
