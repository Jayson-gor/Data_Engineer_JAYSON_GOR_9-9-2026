{#-
  Deterministic surrogate key from one or more columns (NULL-safe).
  Implemented here rather than via dbt_utils so the project has zero external
  package dependencies and runs fully offline.
-#}
{% macro surrogate_key(columns) -%}
    md5(
        {%- for col in columns -%}
            coalesce(cast({{ col }} as {{ dbt.type_string() }}), '')
            {%- if not loop.last %} || '||' || {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}

