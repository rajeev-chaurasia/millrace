{{ config(tags=['candidate']) }}

select
    order_id,
    customer_id,
    ordered_at,
    order_status as status,
    source_updated_at as updated_at,
    source_batch_id as batch_id
from {{ ref('stg_orders') }}
