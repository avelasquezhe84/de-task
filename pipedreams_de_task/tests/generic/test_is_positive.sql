{% test is_positive(model, column_name) %}

    select *
    from {{ model }}
    where try_to_number({{ column_name }}, 38, 2) < 0

{% endtest %}
