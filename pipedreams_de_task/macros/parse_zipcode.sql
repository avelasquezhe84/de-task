{% macro parse_zipcode(column_name) %}
    lpad(split_part(trim({{ column_name }}), '.', 1), 5, '0') as {{ column_name }}
{% endmacro %}