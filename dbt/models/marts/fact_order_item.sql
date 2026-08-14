{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['order_id', 'line_number'],
        on_schema_change='fail'
    )
}}

select
    order_item_id,
    order_id,
    line_number,
    customer_id,
    product_id,
    cast(strftime(order_date, '%Y%m%d') as integer) as order_date_key,
    quantity,
    unit_price,
    line_amount,
    source_updated_at,
    source_batch_id,
    '{{ millrace_run_id() }}' as pipeline_run_id,
    cast({{ millrace_batch_id() }} as bigint) as pipeline_batch_id
from {{ ref('int_order_items_enriched') }}
{% if is_incremental() %}
where source_batch_id >= coalesce(
    (select max(source_batch_id) from {{ this }}),
    0
)
{% endif %}
