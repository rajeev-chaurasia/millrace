{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id',
        on_schema_change='fail'
    )
}}

select
    order_id,
    customer_id,
    {{ millrace_date_key('order_date') }} as order_date_key,
    order_status,
    ordered_at,
    order_total,
    item_quantity,
    item_count,
    source_updated_at,
    source_batch_id,
    '{{ millrace_run_id() }}' as pipeline_run_id,
    cast({{ millrace_batch_id() }} as bigint) as pipeline_batch_id
from {{ ref('int_orders_enriched') }}
{% if is_incremental() %}
where source_batch_id >= coalesce(
    (select max(source_batch_id) from {{ this }}),
    0
)
{% endif %}
