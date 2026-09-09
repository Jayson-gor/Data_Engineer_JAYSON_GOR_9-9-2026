{{ config(unique_key = 'snapshot_date') }}

/*
    BUSINESS QUESTION  How is each route priced, by carrier, cabin and connection type?
    GRAIN              snapshot_date x route x airline x cabin_class x stops
    VALUE              Competitive positioning: where are we the cheapest / most
                       expensive carrier on a sector, and how big is the premium
                       for non-stop vs connecting itineraries.
*/
with fares as (

    select * from {{ ref('fct_fare_observations') }}
    {{ incremental_affected_snapshots(ref('fct_fare_observations')) }}

),

agg as (

    select
        f.snapshot_date,
        r.route,
        r.source_city,
        r.destination_city,
        a.airline_code,
        a.airline_name,
        c.cabin_class,
        f.stops,

        count(*)                                    as fare_count,
        count(distinct f.flight_number)             as distinct_flights,
        min(f.price_inr)                            as min_price_inr,
        {{ median_of('f.price_inr') }}              as median_price_inr,
        round(avg(f.price_inr), 2)                  as avg_price_inr,
        max(f.price_inr)                            as max_price_inr,
        round(stddev_samp(f.price_inr), 2)          as price_stddev_inr,
        round(avg(f.duration_hours), 2)             as avg_duration_hours,
        round(avg(f.price_per_hour_inr), 2)         as avg_price_per_hour_inr,
        max(f._silver_processed_at)                 as _silver_processed_at

    from fares f
    join {{ ref('dim_route') }}       r on r.route_key       = f.route_key
    join {{ ref('dim_airline') }}     a on a.airline_key     = f.airline_key
    join {{ ref('dim_cabin_class') }} c on c.cabin_class_key = f.cabin_class_key
    group by 1, 2, 3, 4, 5, 6, 7, 8

),

ranked as (

    select
        *,
        rank() over (
            partition by snapshot_date, route, cabin_class, stops
            order by median_price_inr
        ) as price_rank_on_route,
        round(
            median_price_inr - min(median_price_inr) over (partition by snapshot_date, route, cabin_class, stops),
            2
        ) as premium_vs_cheapest_inr
    from agg

)

select * from ranked