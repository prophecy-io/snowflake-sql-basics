{%- macro IntersectOperation(relation, operationType) -%}
    {{ log("Parameters are: " ~ relation ~ ", " ~ operationType, info=True) }}

    {# Split and filter the relations list #}
    {%- set relations = relation.split(',') | map('trim') | reject('equalto', '') | list %}

    {# Determine the union operator based on operationType input #}
    {%- if operationType | lower == "intersect" %}
        {%- set op = "INTERSECT" %}
    {%- else %}
        {%- set op = operationType %}
    {%- endif %}

    {# Create a namespace to hold the accumulating SQL statement #}
    {%- set ns = namespace(sql_statement="") %}

    {%- for rel in relations %}
        {%- set relName = "\"" ~ rel ~ "\"" %}
        {%- if loop.first %}
            {%- set ns.sql_statement = "SELECT * FROM " ~ relName %}
        {%- else %}
            {%- set ns.sql_statement = ns.sql_statement ~ " " ~ op ~ " SELECT * FROM " ~ relName %}
        {%- endif %}
    {%- endfor %}

    {{ log("Final SQL statement is: " ~ ns.sql_statement, info=True) }}
    {{ return(ns.sql_statement) }}
{%- endmacro %}