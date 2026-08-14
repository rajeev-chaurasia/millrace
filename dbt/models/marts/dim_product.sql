{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='product_id',
        on_schema_change='fail'
    )
}}

select
    product_id,
    sku,
    product_name,
    category,
    unit_price,
    is_active,
    source_updated_at,
    source_batch_id,
    '{{ millrace_run_id() }}' as pipeline_run_id,
    cast({{ millrace_batch_id() }} as bigint) as pipeline_batch_id
from {{ ref('stg_products') }}
{% if is_incremental() %}
where source_batch_id >= coalesce(
    (select max(source_batch_id) from {{ this }}),
    0
)
{% endif %}
