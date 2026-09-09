{% test positive_value(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is null or {{ column_name }} <= 0
{% endtest %}