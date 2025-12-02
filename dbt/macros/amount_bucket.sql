{% macro amount_bucket(amount_col) %}
    case
        when {{ amount_col }} is null then 'unknown'
        when {{ amount_col }} < 50 then 'small'
        when {{ amount_col }} < 200 then 'medium'
        when {{ amount_col }} < 500 then 'large'
        else 'extra_large'
    end
{% endmacro %}
