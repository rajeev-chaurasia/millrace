{% test non_negative(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} < 0
{% endtest %}

{% test positive(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} <= 0
{% endtest %}

{% test unique_combination_of_columns(model, column_names) %}
    select
        {% for column_name in column_names %}
            {{ column_name }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ model }}
    group by
        {% for column_name in column_names %}
            {{ column_name }}{% if not loop.last %},{% endif %}
        {% endfor %}
    having count(*) > 1
{% endtest %}
