{% set run_id = millrace_run_id() %}

select
    concat(cast(order_id as varchar), ':', cast(line_number as varchar)) as order_item_id,
    cast(order_id as bigint) as order_id,
    cast(line_number as integer) as line_number,
    cast(product_id as bigint) as product_id,
    cast(quantity as bigint) as quantity,
    cast(unit_price as decimal(18, 2)) as unit_price,
    cast(updated_at as timestamptz) as source_updated_at,
    cast(batch_id as bigint) as source_batch_id
from {{ source('silver', 'order_items') }}
where cast(batch_id as bigint) <= {{ millrace_batch_id() }}
