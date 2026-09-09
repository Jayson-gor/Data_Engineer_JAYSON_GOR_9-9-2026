{{ config(unique_key = 'snapshot_date') }}

/*
    BUSINESS QUESTION  How do carriers compare on network breadth, product mix,
                       connection quality and price positioning?
    GRAIN              snapshot_date x airline x cabin_class
    VALUE              Executive benchmarking scorecard: fare-inventory share,
                       non-stop ratio, route coverage, price-per-hour efficiency.
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
        a.carrier_type,
        c.cabin_class,

        count(*)                                                    as fare_count,
        count(distinct f.route_key)                                 as routes_served,
        count(distinct f.flight_number)                             as distinct_flights,
        round(100.0 * avg(case when f.is_nonstop then 1.0 else 0.0 end), 2) as nonstop_pct,
        round(avg(f.stops), 3)                                      as avg_stops,
        round(avg(f.duration_hours), 2)                             as avg_duration_hours,
        {{ median_of('f.price_inr') }}                              as median_price_inr,
        round(avg(f.price_inr), 2)                                  as avg_price_inr,
        round(avg(f.price_per_hour_inr), 2)                         as avg_price_per_hour_inr,
        round(100.0 * avg(case when f.has_dq_warning then 1.0 else 0.0 end), 2) as dq_warning_pct,
        max(f._silver_processed_at)                                 as _silver_processed_at

    from fares f
    join {{ ref('dim_airline') }}     a on a.airline_key     = f.airline_key
    join {{ ref('dim_cabin_class') }} c on c.cabin_class_key = f.cabin_class_key
    group by 1, 2, 3, 4, 5

)

select
    *,
    round(100.0 * fare_count / sum(fare_count) over (partition by snapshot_date, cabin_class), 2) as fare_share_pct,
    rank() over (partition by snapshot_date, cabin_class order by median_price_inr)                as price_rank
from agg