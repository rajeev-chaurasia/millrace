{% set run_id = millrace_run_id() %}

select
    cast(product_id as bigint) as product_id,
    nullif(trim(cast(sku as varchar)), '') as sku,
    nullif(trim(cast(name as varchar)), '') as product_name,
    nullif(trim(cast(category as varchar)), '') as category,
    cast(unit_price as decimal(18, 2)) as unit_price,
    cast(active as boolean) as is_active,
    {{ millrace_timestamp_tz('updated_at') }} as source_updated_at,
    cast(batch_id as bigint) as source_batch_id
from {{ source('silver', 'products') }}
where cast(batch_id as bigint) <= {{ millrace_batch_id() }}
