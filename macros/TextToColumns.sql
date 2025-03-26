
{%- macro TextToColumns(parameter1,relation_name) -%}
{{ log("The relation name is " ~ relation_name, info = True) }}
    select * from {{ parameter1 }}
{%- endmacro -%}
