{#-
  Use the configured custom schema verbatim (staging / marts) instead of dbt's
  default "<target_schema>_<custom_schema>" concatenation, so object names are
  identical on DuckDB and Redshift.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}