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
    {{ millrace_date_spine('bounds') }}
)

select
    {{ millrace_date_key('date_day') }} as date_key,
    date_day,
    year(date_day) as year_number,
    quarter(date_day) as quarter_number,
    month(date_day) as month_number,
    {{ millrace_month_name('date_day') }} as month_name,
    {{ millrace_iso_week('date_day') }} as week_number,
    day(date_day) as day_of_month,
    {{ millrace_iso_dow('date_day') }} as day_of_week,
    {{ millrace_day_name('date_day') }} as day_name,
    {{ millrace_iso_dow('date_day') }} in (6, 7) as is_weekend,
    '{{ millrace_run_id() }}' as pipeline_run_id,
    cast({{ millrace_batch_id() }} as bigint) as pipeline_batch_id
from date_spine
