
{%- macro ColumnParser(relation, parserType, columnToParse, schema) -%}
    
    {% if parserType | lower == "json" %}
    
    select *, PARSE_JSON ( {{ '"' ~ columnToParse ~ '"' }} ) as json_parsed_content from {{ relation }}
    
    {% else %}

    select *, PARSE_XML ( {{ '"' ~ columnToParse ~ '"' }} ) as xml_parsed_content from {{ relation }}
    
    {% endif %}
    
{%- endmacro -%}
