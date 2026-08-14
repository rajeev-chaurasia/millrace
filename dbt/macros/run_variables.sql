{% macro millrace_required_var(name) -%}
    {%- set environment_name = 'MILLRACE_' ~ name | upper -%}
    {%- set value = var(name, env_var(environment_name, '')) -%}
    {%- if value | string | trim == '' -%}
        {{ exceptions.raise_compiler_error("Required dbt variable '" ~ name ~ "' is missing") }}
    {%- endif -%}
    {{ return(value) }}
{%- endmacro %}

{% macro millrace_run_id() -%}
    {%- set value = millrace_required_var('run_id') | string -%}
    {%- if not modules.re.match('^[A-Za-z0-9][A-Za-z0-9_.-]*$', value) -%}
        {{ exceptions.raise_compiler_error(
            "run_id must contain only letters, numbers, periods, underscores, and hyphens"
        ) }}
    {%- endif -%}
    {{ return(value) }}
{%- endmacro %}

{% macro millrace_batch_id() -%}
    {%- set value = millrace_required_var('batch_id') | string -%}
    {%- if not modules.re.match('^[1-9][0-9]*$', value) -%}
        {{ exceptions.raise_compiler_error("batch_id must be a positive integer") }}
    {%- endif -%}
    {{ return(value) }}
{%- endmacro %}

{% macro millrace_interval_start() -%}
    {{ return(millrace_required_var('data_interval_start') | string) }}
{%- endmacro %}

{% macro millrace_interval_end() -%}
    {{ return(millrace_required_var('data_interval_end') | string) }}
{%- endmacro %}
