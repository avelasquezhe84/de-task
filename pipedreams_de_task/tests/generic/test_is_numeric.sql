{% test is_numeric(model, column_name) %}

    select *
    from {{ model }}
    where not is_decimal(try_to_number({{ column_name }}, 38, 2))

{% endtest %}