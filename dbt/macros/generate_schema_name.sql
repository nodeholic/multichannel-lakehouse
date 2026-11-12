{#
  By default dbt prefixes custom schemas with the target schema ("main_gold").
  We want the plain layer names ("staging", "gold") so queries read naturally:
    SELECT * FROM gold.fact_order_items
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
