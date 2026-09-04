{% set run_id = millrace_run_id() %}

select
    cast(customer_id as bigint) as customer_id,
    nullif(trim(cast(first_name as varchar)), '') as first_name,
    nullif(trim(cast(last_name as varchar)), '') as last_name,
    lower(nullif(trim(cast(email as varchar)), '')) as email,
    lower(nullif(trim(cast(status as varchar)), '')) as customer_status,
    {{ millrace_timestamp_tz('updated_at') }} as source_updated_at,
    cast(batch_id as bigint) as source_batch_id
from {{ source('silver', 'customers') }}
where cast(batch_id as bigint) <= {{ millrace_batch_id() }}
