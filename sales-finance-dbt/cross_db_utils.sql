{#
  cross_db_utils.sql
  ───────────────────
  Thin wrappers around date functions that differ between DuckDB (dev)
  and Snowflake (prod). Add new functions here as needed rather than
  scattering adapter-specific logic across models.

  Usage:
    {{ dbt_utils.dateadd('day', -90, 'current_date()') }}  -- use dbt_utils for dateadd
    {{ day_of_week('order_date') }}                         -- use this macro
#}

{% macro day_of_week(date_col) %}
  {% if target.type == 'duckdb' %}
    dayofweek({{ date_col }})
  {% else %}
    dayofweek({{ date_col }})
  {% endif %}
{% endmacro %}

{% macro week_of_year(date_col) %}
  {% if target.type == 'duckdb' %}
    weekofyear({{ date_col }})
  {% else %}
    weekofyear({{ date_col }})
  {% endif %}
{% endmacro %}

{% macro make_date(year_col, month_col, day_val=1) %}
  {# DuckDB: make_date(year, month, day). Snowflake: date_from_parts(year, month, day) #}
  {% if target.type == 'duckdb' %}
    make_date({{ year_col }}::int, {{ month_col }}::int, {{ day_val }})
  {% else %}
    date_from_parts({{ year_col }}, {{ month_col }}, {{ day_val }})
  {% endif %}
{% endmacro %}

{% macro title_case(col) %}
  {# DuckDB uses initcap, Snowflake also supports initcap — same for both #}
  initcap(trim({{ col }}))
{% endmacro %}
