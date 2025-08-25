{#
Macro: extract_resource_id

Extracts the id that follows a given "key" segment from a slash-delimited path:
  Example path: 'customers/1416177352/campaigns/19906364320'
  - key 'customers' -> '1416177352'
  - key 'campaigns' -> '19906364320'

Args:
  resource_col (string): SQL expression or column name containing the path.
  key (string): The segment to find (e.g., 'campaigns'). Pass as a SQL string literal: "'campaigns'".
  coalesce_with (string, optional): SQL literal to coalesce with if not found, e.g., "'unknown'" or "NULL". Defaults to NULL (no coalesce).

Returns:
  A SQL expression that yields the id as a string type on the current warehouse.
#}
{% macro extract_resource_id(resource_col, key, coalesce_with=None) %}
  {# Build the regex for matching the id after the given key. Pattern:
     (?:^|/)key/([^/]+)(?:/|$)
     Capture group 1 is the id.
  #}

  {% set pattern_concat = "CONCAT('(?:^|/)', " ~ key ~ ", '/([^/]+)(?:/|$)')" %}

  {# Warehouse-specific extraction expression #}
  {% if target.type == 'bigquery' %}
    {% set expr %}REGEXP_EXTRACT({{ resource_col }}, {{ pattern_concat }}){% endset %}
  {% elif target.type == 'duckdb' %}
    {% set expr %}REGEXP_EXTRACT({{ resource_col }}, {{ pattern_concat }}, 1){% endset %}
  {% else %}
    {# Fallback: attempt a REGEXP_EXTRACT-compatible syntax #}
    {% set expr %}REGEXP_EXTRACT({{ resource_col }}, {{ pattern_concat }}){% endset %}
  {% endif %}

  {# Return as string, with optional coalesce #}
  {% if coalesce_with is not none %}
    coalesce(cast({{ expr }} as {{ dbt.type_string() }}), {{ coalesce_with }})
  {% else %}
    cast({{ expr }} as {{ dbt.type_string() }})
  {% endif %}
{% endmacro %}
