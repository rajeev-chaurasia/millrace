with checks as (
    select
        'customer' as entity_name,
        (select count(*) from {{ ref('stg_customers') }}) as expected_count,
        (select count(*) from {{ ref('dim_customer') }}) as actual_count
    union all
    select
        'product' as entity_name,
        (select count(*) from {{ ref('stg_products') }}) as expected_count,
        (select count(*) from {{ ref('dim_product') }}) as actual_count
    union all
    select
        'order' as entity_name,
        (select count(*) from {{ ref('stg_orders') }}) as expected_count,
        (select count(*) from {{ ref('fact_order') }}) as actual_count
    union all
    select
        'order_item' as entity_name,
        (select count(*) from {{ ref('stg_order_items') }}) as expected_count,
        (select count(*) from {{ ref('fact_order_item') }}) as actual_count
)

select
    entity_name,
    expected_count,
    actual_count,
    actual_count - expected_count as count_difference
from checks
where expected_count <> actual_count
