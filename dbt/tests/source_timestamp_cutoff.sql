select
    'customers' as source_name,
    cast(customer_id as varchar) as record_key,
    updated_at as source_updated_at
from {{ source('silver', 'customers') }}
where cast(updated_at as timestamp) >= cast('{{ millrace_interval_end() }}' as timestamp)
union all
select
    'products' as source_name,
    cast(product_id as varchar) as record_key,
    updated_at as source_updated_at
from {{ source('silver', 'products') }}
where cast(updated_at as timestamp) >= cast('{{ millrace_interval_end() }}' as timestamp)
union all
select
    'orders' as source_name,
    cast(order_id as varchar) as record_key,
    updated_at as source_updated_at
from {{ source('silver', 'orders') }}
where cast(updated_at as timestamp) >= cast('{{ millrace_interval_end() }}' as timestamp)
union all
select
    'order_items' as source_name,
    concat(cast(order_id as varchar), ':', cast(line_number as varchar)) as record_key,
    updated_at as source_updated_at
from {{ source('silver', 'order_items') }}
where cast(updated_at as timestamp) >= cast('{{ millrace_interval_end() }}' as timestamp)
