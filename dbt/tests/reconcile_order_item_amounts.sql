select
    order_item_id,
    cast(quantity * unit_price as decimal(18, 2)) as expected_amount,
    line_amount as actual_amount,
    line_amount - cast(quantity * unit_price as decimal(18, 2)) as amount_difference
from {{ ref('fact_order_item') }}
where abs(line_amount - cast(quantity * unit_price as decimal(18, 2))) > 0.01
