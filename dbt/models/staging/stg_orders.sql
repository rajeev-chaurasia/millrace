{% set run_id = millrace_run_id() %}

select
    cast(order_id as bigint) as order_id,
    cast(customer_id as bigint) as customer_id,
    lower(nullif(trim(cast(status as varchar)), '')) as order_status,
    cast(ordered_at as timestamptz) as ordered_at,
    cast(updated_at as timestamptz) as source_updated_at,
    cast(batch_id as bigint) as source_batch_id
from {{ source('silver', 'orders') }}
where cast(batch_id as bigint) <= {{ millrace_batch_id() }}
