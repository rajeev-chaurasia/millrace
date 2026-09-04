{#
    Warehouse-portable replacements for constructs that either do not exist on
    Snowflake (generate_series, timestamptz) or that exist on both engines but
    disagree in meaning (dayofweek is WEEK_START-dependent on Snowflake;
    dayname/monthname return abbreviations there and full names on DuckDB).
    dim_date is a published mart the cross-engine oracle compares, so every
    macro here must emit one canonical value regardless of target.type.
#}

{% macro millrace_date_key(column) %}
    {%- if target.type == 'snowflake' -%}
        to_number(to_char({{ column }}, 'YYYYMMDD'))
    {%- else -%}
        cast(strftime({{ column }}, '%Y%m%d') as integer)
    {%- endif -%}
{% endmacro %}

{% macro millrace_timestamp_tz(expr) %}
    {%- if target.type == 'snowflake' -%}
        cast({{ expr }} as timestamp_tz)
    {%- else -%}
        cast({{ expr }} as timestamptz)
    {%- endif -%}
{% endmacro %}

{#
    `bounds_relation` must expose exactly one row with start_date and end_date
    date columns. Snowflake has no generate_series table function, so a bound
    row generator (GENERATOR + SEQ4) stands in, capped at 100000 rows
    (roughly 273 years) and filtered down to the requested range.
#}
{% macro millrace_date_spine(bounds_relation) %}
    {%- if target.type == 'snowflake' -%}
select cast(dateadd('day', seq4(), b.start_date) as date) as date_day
from {{ bounds_relation }} as b
cross join table(generator(rowcount => 100000)) as g
where dateadd('day', seq4(), b.start_date) <= b.end_date
    {%- else -%}
select cast(dates.generated_date as date) as date_day
from {{ bounds_relation }}
cross join generate_series(start_date, end_date, interval '1 day') as dates (generated_date)
    {%- endif -%}
{% endmacro %}

{#
    ISO day of week, Monday=1..Sunday=7. DuckDB's dayofweek() is Sunday=0
    based and Snowflake's is governed by the session WEEK_START parameter, so
    neither is safe to use directly on a published column.
#}
{% macro millrace_iso_dow(column) %}
    {%- if target.type == 'snowflake' -%}
        dayofweekiso({{ column }})
    {%- else -%}
        isodow({{ column }})
    {%- endif -%}
{% endmacro %}

{#
    ISO-8601 week number. DuckDB's week() is already ISO by default;
    Snowflake's week() instead follows WEEK_START/WEEK_OF_YEAR_POLICY, so this
    uses weekiso() there, which is defined independently of those settings.
#}
{% macro millrace_iso_week(column) %}
    {%- if target.type == 'snowflake' -%}
        weekiso({{ column }})
    {%- else -%}
        week({{ column }})
    {%- endif -%}
{% endmacro %}

{#
    dayname()/monthname() return full names on DuckDB and three-letter
    abbreviations on Snowflake. A literal lookup on the numeric day/month is
    locale-independent and identical on both engines instead of relying on
    either builtin's formatting.
#}
{% macro millrace_day_name(column) %}
    case {{ millrace_iso_dow(column) }}
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        when 6 then 'Saturday'
        when 7 then 'Sunday'
    end
{% endmacro %}

{% macro millrace_month_name(column) %}
    case month({{ column }})
        when 1 then 'January'
        when 2 then 'February'
        when 3 then 'March'
        when 4 then 'April'
        when 5 then 'May'
        when 6 then 'June'
        when 7 then 'July'
        when 8 then 'August'
        when 9 then 'September'
        when 10 then 'October'
        when 11 then 'November'
        when 12 then 'December'
    end
{% endmacro %}
