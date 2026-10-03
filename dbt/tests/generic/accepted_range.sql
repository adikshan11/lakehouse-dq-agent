{% test accepted_range(model, column_name, min_value=none, max_value=none) %}
select *
from {{ model }}
where
{%- if min_value is not none %} {{ column_name }} < {{ min_value }}{% endif %}
{%- if min_value is not none and max_value is not none %} or{% endif %}
{%- if max_value is not none %} {{ column_name }} > {{ max_value }}{% endif %}
{%- if min_value is none and max_value is none %} false{% endif %}
{% endtest %}
