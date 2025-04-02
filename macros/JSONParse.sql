{% macro JSONParse(
    relation_name,
    columnName
    ) %}

    select
        *,
        {%- for col in columnName %}
            PARSE_JSON({{ '"' ~ col ~ '"' }}) as {{ '"json_parsed_' ~ col ~ '"' }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ relation_name }}
    
{% endmacro %}