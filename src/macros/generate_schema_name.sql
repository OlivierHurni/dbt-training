{#
  By default dbt builds the schema as <target schema>_<custom schema>.
  Here the custom schema (e.g. `sports_shop`) is used as is, so the objects land in
  silver.sports_shop and gold.sports_shop regardless of the target.
  Models without a custom schema fall back to the target schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
