{% macro XMLParse(
    relation_name,
    columnName
    ) %}

    select
        *,
        {%- for col in columnName %}
            PARSE_XML({{ '"' ~ col ~ '"' }})::VARIANT as {{ '"' ~ col ~ '_parsed"' }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ relation_name }}

{% endmacro %}