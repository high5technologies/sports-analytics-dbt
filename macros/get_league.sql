{% macro get_league(node) -%}

    {# Read name and try to parse string #}
    {% set node_list = node.name.split('__') %}
    {%- if node_list|length > 1 -%}
        {% set league = node_list[0].lower() %}
    {% else %}
        {% set league = "none" %}
    {% endif %}

    {{ league }}

{%- endmacro %}