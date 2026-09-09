{#-
  Standard incremental predicate for fact/mart models.

  "Affected snapshots" = every snapshot_date in the upstream relation that has
  at least one row processed AFTER the watermark already held in {{ this }}.
  The model then re-selects ALL rows for those snapshot_dates and the
  materialization (delete+insert on unique_key = snapshot_date) swaps the
  partitions atomically. This makes one code path handle:
    * new daily files            (new snapshot_date)
    * late-arriving files        (old snapshot_date, new processed_at)
    * corrected re-deliveries    (same snapshot_date, superseded batch)
    * reruns                     (nothing newer than watermark -> zero rows -> no-op)
  and guarantees no double counting regardless of how many times it runs.
-#}
{% macro incremental_affected_snapshots(upstream, ts_column='_silver_processed_at', date_column='snapshot_date') -%}
    {%- if is_incremental() -%}
        where {{ date_column }} in (
            select distinct {{ date_column }}
            from {{ upstream }}
            where {{ ts_column }} > (
                select coalesce(max({{ ts_column }}), cast('1900-01-01' as timestamp)) from {{ this }}
            )
        )
    {%- endif -%}
{%- endmacro %}
