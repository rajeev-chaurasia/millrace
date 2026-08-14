select
    'customers' as source_name,
    cast(customer_id as varchar) as record_key,
    batch_id as source_batch_id
from {{ source('silver', 'customers') }}
where cast(batch_id as bigint) > {{ millrace_batch_id() }}
union all
select
    'products' as source_name,
    cast(product_id as varchar) as record_key,
    batch_id as source_batch_id
from {{ source('silver', 'products') }}
where cast(batch_id as bigint) > {{ millrace_batch_id() }}
union all
select
    'orders' as source_name,
    cast(order_id as varchar) as record_key,
    batch_id as source_batch_id
from {{ source('silver', 'orders') }}
where cast(batch_id as bigint) > {{ millrace_batch_id() }}
union all
select
    'order_items' as source_name,
    concat(cast(order_id as varchar), ':', cast(line_number as varchar)) as record_key,
    batch_id as source_batch_id
from {{ source('silver', 'order_items') }}
where cast(batch_id as bigint) > {{ millrace_batch_id() }}
