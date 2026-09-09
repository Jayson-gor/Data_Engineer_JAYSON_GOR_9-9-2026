{#-
  Adapter-specific SQL isolated here so models stay identical across
  DuckDB (local) and Redshift (production).
-#}

{% macro date_key(col) -%}
    {{ return(adapter.dispatch('date_key', 'airline_analytics')(col)) }}
{%- endmacro %}

{% macro default__date_key(col) -%}
    cast(to_char({{ col }}, 'YYYYMMDD') as integer)
{%- endmacro %}

{% macro duckdb__date_key(col) -%}
    cast(strftime({{ col }}, '%Y%m%d') as integer)
{%- endmacro %}


{% macro day_name(col) -%}
    {{ return(adapter.dispatch('day_name', 'airline_analytics')(col)) }}
{%- endmacro %}

{% macro default__day_name(col) -%}
    trim(to_char({{ col }}, 'Day'))
{%- endmacro %}

{% macro duckdb__day_name(col) -%}
    strftime({{ col }}, '%A')
{%- endmacro %}


{% macro median_of(col) -%}
    {{ return(adapter.dispatch('median_of', 'airline_analytics')(col)) }}
{%- endmacro %}

{% macro default__median_of(col) -%}
    median({{ col }})
{%- endmacro %}