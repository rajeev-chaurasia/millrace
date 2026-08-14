{{ config(tags=['candidate']) }}

select
    customer_id,
    email,
    first_name,
    last_name,
    source_updated_at as updated_at,
    source_batch_id as batch_id
from {{ ref('stg_customers') }}
