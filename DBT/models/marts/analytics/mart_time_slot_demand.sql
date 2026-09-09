{{ config(unique_key = 'snapshot_date') }}

/*
    BUSINESS QUESTION  Which departure windows are most served and most expensive
                       on each route?
    GRAIN              snapshot_date x route x departure_time_slot x cabin_class
    VALUE              Schedule planning: reveals under-served time bands (few
                       fares, high prices = opportunity) and over-supplied bands.
*/
with fares as (

    select * from {{ ref('fct_fare_observations') }}
    {{ incremental_affected_snapshots(ref('fct_fare_observations')) }}

),

agg as (

    select
        f.snapshot_date,
        r.route,
        t.time_slot                                 as departure_time_slot,
        t.slot_order                                as departure_slot_order,
        t.approx_time_window,
        c.cabin_class,

        count(*)                                    as fare_count,
        count(distinct f.airline_key)               as airlines_operating,
        count(distinct f.flight_number)             as distinct_flights,
        {{ median_of('f.price_inr') }}              as median_price_inr,
        round(avg(f.price_inr), 2)                  as avg_price_inr,
        round(100.0 * avg(case when f.is_nonstop then 1.0 else 0.0 end), 2) as nonstop_pct,
        max(f._silver_processed_at)                 as _silver_processed_at

    from fares f
    join {{ ref('dim_route') }}       r on r.route_key       = f.route_key
    join {{ ref('dim_time_slot') }}   t on t.time_slot_key   = f.departure_slot_key
    join {{ ref('dim_cabin_class') }} c on c.cabin_class_key = f.cabin_class_key
    group by 1, 2, 3, 4, 5, 6

)

select
    *,
    round(100.0 * fare_count / sum(fare_count) over (partition by snapshot_date, route, cabin_class), 2) as slot_share_pct
from agg