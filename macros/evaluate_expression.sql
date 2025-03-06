{% macro evaluate_expression(expression,column) %}

{% set sql_query = 'select ' ~ expression ~ ' as result' %}
{% set result = run_query(sql_query) %}
 
{% if result %}
  {%- if execute -%}
    {% for row in result.rows %}
      {{row[0]}}
    {% endfor %}
  {%- endif -%}
{% else %}
    {{ column }}
{% endif %}
{% endmacro %}