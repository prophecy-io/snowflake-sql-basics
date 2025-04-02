{% macro XMLParse(
    relation_name,
    columnName
    ) %}

    select
        *,
        {%- for col in columnName %}
            PARSE_XML({{ '"' ~ col ~ '"' }}) as {{ '"xml_parsed_' ~ col ~ '"' }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ relation_name }}

{% endmacro %}