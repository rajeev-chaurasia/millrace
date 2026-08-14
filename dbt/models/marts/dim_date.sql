{{ config(materialized='table') }}

with bounds as (
    select
        least(
            coalesce(min(order_date), cast('{{ millrace_interval_start() }}' as date)),
            cast('{{ millrace_interval_start() }}' as date)
        ) as start_date,
        greatest(
            coalesce(max(order_date), cast('{{ millrace_interval_end() }}' as date)),
            cast('{{ millrace_interval_end() }}' as date)
        ) as end_date
    from {{ ref('int_orders_enriched') }}
),

date_spine as (
    select cast(generated_date as date) as date_day
    from bounds
    cross join generate_series(
        start_date,
        end_date,
        interval '1 day'
    ) as dates (generated_date)
)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    year(date_day) as year_number,
    quarter(date_day) as quarter_number,
    month(date_day) as month_number,
    monthname(date_day) as month_name,
    week(date_day) as week_number,
    day(date_day) as day_of_month,
    dayofweek(date_day) as day_of_week,
    dayname(date_day) as day_name,
    dayofweek(date_day) in (0, 6) as is_weekend,
    '{{ millrace_run_id() }}' as pipeline_run_id,
    cast({{ millrace_batch_id() }} as bigint) as pipeline_batch_id
from date_spine
