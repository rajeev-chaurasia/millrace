{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        on_schema_change='fail'
    )
}}

select
    customer_id,
    first_name,
    last_name,
    nullif(trim(concat_ws(' ', first_name, last_name)), '') as customer_name,
    email,
    customer_status,
    source_updated_at,
    source_batch_id,
    '{{ millrace_run_id() }}' as pipeline_run_id,
    cast({{ millrace_batch_id() }} as bigint) as pipeline_batch_id
from {{ ref('stg_customers') }}
{% if is_incremental() %}
where source_batch_id >= coalesce(
    (select max(source_batch_id) from {{ this }}),
    0
)
{% endif %}
