{% macro generate_schema_name(custom_schema_name, node) -%}

    {# Read name and try to parse string #}
    {% set node_list = node.name.split('__') %}
    {%- if node_list|length > 1 -%}
        {% set schema_parsed = node_list[0].lower() %}
    {% else %}
        {% set schema_parsed = target.schema.lower() %}
    {% endif %}


    {# Add developer prefix to schema in dev #}
    {%- if target.name.lower() is in (['default'])  -%}
        {% set deveveloper_schema = target.schema.lower() ~ '_' %}
    {% else %}
        {% set deveveloper_schema = '' %}
    {% endif %}

    {# Use custom schema if passed in, else use schema from node name #}
    {% if custom_schema_name is not none %} 
        {% set schema_string = custom_schema_name %}
    {% else %}
        {% set schema_string = schema_parsed %}
    {% endif %}

    {{ deveveloper_schema ~ schema_string }}

{%- endmacro %}