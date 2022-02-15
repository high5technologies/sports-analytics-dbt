{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {# Read name and try to parse string #}
    {% set node_list = node.name.split('__') %}
    {%- if node_list|length > 1 -%}
        {% set object_parsed = node_list[1].lower() %}
    {% else %}
        {% set object_parsed = node.name.lower() %}
    {% endif %}

    {# Check for custom name passed in, else use parsed object name above #}
    {%- if custom_alias_name is none -%}
        {{ object_parsed }}
    {%- else -%}
        {{ custom_alias_name | trim }}
    {%- endif -%}

{%- endmacro %}