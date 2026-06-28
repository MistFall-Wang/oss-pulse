{#
  Register external Delta paths as Spark databases + tables so that
  dbt's built-in source() function and its source schema tests work.

  Pattern: for each declared source, look up var('<source>_<table>_path')
  and CREATE TABLE ... USING DELTA LOCATION '<path>'.

  This runs on every `dbt run` / `dbt test` invocation. Idempotent because
  we use CREATE TABLE IF NOT EXISTS / CREATE SCHEMA IF NOT EXISTS.
#}
{% macro register_external_sources() %}
    {% if execute %}
        {% set sources_to_register = [
            ('bronze', 'events', var('bronze_events_path', none)),
        ] %}

        {% for source_name, table_name, path in sources_to_register %}
            {% if path %}
                {% do run_query("CREATE SCHEMA IF NOT EXISTS " ~ source_name) %}
                {% do run_query(
                    "CREATE TABLE IF NOT EXISTS " ~ source_name ~ "." ~ table_name ~
                    " USING DELTA LOCATION '" ~ path ~ "'"
                ) %}
                {{ log("[register_external_sources] registered " ~ source_name ~ "." ~ table_name ~ " -> " ~ path, info=True) }}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
