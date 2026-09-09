{{ config(unique_key = 'snapshot_date') }}

/*
    BUSINESS QUESTION  How does price move as departure approaches, per carrier
                       and cabin?  (advance-purchase curve)
    GRAIN              snapshot_date x airline x cabin_class x booking_window
    VALUE              Revenue management: quantifies the last-minute premium,
                       identifies the cheapest booking window to advertise, and
                       benchmarks our fare ladder against competitors.
*/
with fares as (

    select * from {{ ref('fct_fare_observations') }}
    {{ incremental_affected_snapshots(ref('fct_fare_observations')) }}

),

agg as (

    select
        f.snapshot_date,
        a.airline_code,
        a.airline_name,
        c.cabin_class,
        f.booking_window,
        f.booking_window_order,

        count(*)                                    as fare_count,
        min(f.days_left)                            as min_days_left,
        max(f.days_left)                            as max_days_left,
        {{ median_of('f.price_inr') }}              as median_price_inr,
        round(avg(f.price_inr), 2)                  as avg_price_inr,
        min(f.price_inr)                            as min_price_inr,
        max(f.price_inr)                            as max_price_inr,
        max(f._silver_processed_at)                 as _silver_processed_at

    from fares f
    join {{ ref('dim_airline') }}     a on a.airline_key     = f.airline_key
    join {{ ref('dim_cabin_class') }} c on c.cabin_class_key = f.cabin_class_key
    group by 1, 2, 3, 4, 5, 6

),

with_baseline as (

    select
        *,
        -- the 31+ day window is the "book early" baseline for the carrier/cabin
        max(case when booking_window_order = 5 then median_price_inr end)
            over (partition by snapshot_date, airline_code, cabin_class)      as early_booking_median_inr
    from agg

)

select
    *,
    round(median_price_inr - early_booking_median_inr, 2)                                          as premium_vs_early_inr,
    round(100.0 * (median_price_inr - early_booking_median_inr) / nullif(early_booking_median_inr, 0), 2) as premium_vs_early_pct
from with_baseline