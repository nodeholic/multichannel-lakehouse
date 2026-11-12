{#
  Surrogate key = md5 of the natural key column(s), joined with '|'.

  Why a hash instead of a row_number() counter: Gold tables are fully rebuilt on every run.
  A counter would renumber rows whenever a new natural key sorts in between existing ones,
  silently re-pointing every fact row. A hash of the natural key is the same on every run,
  on every machine, and in any database that has md5 (DuckDB today, Postgres in phase 3).

  Usage: {{ surrogate_key(['product_id']) }}
#}
{% macro surrogate_key(columns) -%}
    md5(concat_ws('|', {%- for col in columns %} coalesce(cast({{ col }} as varchar), '') {%- if not loop.last %},{% endif %}{%- endfor %}))
{%- endmacro %}
