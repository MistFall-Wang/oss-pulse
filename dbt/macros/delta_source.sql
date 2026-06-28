{#
  delta_source: thin wrapper around dbt's source() that we keep
  for forward compatibility with cloud catalogs (Sprint 5+).

  External Delta paths are registered as Spark tables via the
  register_external_sources() macro in on-run-start hook.
#}
{% macro delta_source(source_name, table_name) %}
    {{ source(source_name, table_name) }}
{% endmacro %}
