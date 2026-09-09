{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'snapshot_date',
        on_schema_change = 'append_new_columns'
    )
}}

/*
    GRAIN: one row per distinct fare observation, i.e. per
           (snapshot_date, airline, flight_number, route, departure slot, arrival slot,
            stops, cabin class, days_left, price).
    Exact duplicates were collapsed in Silver (duplicate_count keeps the evidence).
    Same itinerary observed at a different price on the same day is a separate row
    on purpose - it is a different fare, not a duplicate.

    INCREMENTAL: whole snapshot_date partitions are replaced (see macro docs).
*/
with enriched as (

    select * from {{ ref('int_fare_observations_enriched') }}
    {{ incremental_affected_snapshots(ref('stg_silver__flight_fares')) }}

)

select
    -- keys ------------------------------------------------------------------
    fare_record_key,
    snapshot_date,
    {{ date_key('snapshot_date') }}     as snapshot_date_key,
    airline_key,
    route_key,
    cabin_class_key,
    departure_slot_key,
    arrival_slot_key,

    -- degenerate dimensions ---------------------------------------------------
    flight_number,
    stops,
    is_nonstop,
    days_left,
    booking_window,
    booking_window_order,

    -- measures ---------------------------------------------------------------
    price_inr,
    duration_hours,
    price_per_hour_inr,
    duplicate_count,

    -- quality / lineage ----------------------------------------------------
    has_dq_warning,
    dq_warnings,
    batch_id,
    source_file,
    ingested_at,
    _silver_processed_at

from enriched