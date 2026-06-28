{# Override dbt's default schema name generation so custom_schema_name
   is used verbatim (e.g. 'gold'), not prefixed with the target schema
   (which would yield 'silver_gold' in our dev profile).

   Reference: https://docs.getdbt.com/docs/build/custom-schemas
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
