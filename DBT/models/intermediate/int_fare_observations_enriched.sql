-- Ephemeral: adds derived attributes and conformed dimension keys that every
-- downstream mart needs, so the logic lives in exactly one place.
with fares as (

    select * from {{ ref('stg_silver__flight_fares') }}

),

enriched as (

    select
        f.*,

        -- conformed dimension keys -------------------------------------------
        {{ surrogate_key(['f.airline_code']) }}                                     as airline_key,
        {{ surrogate_key(['f.source_city', 'f.destination_city']) }}                as route_key,
        {{ surrogate_key(['f.cabin_class']) }}                                      as cabin_class_key,
        {{ surrogate_key(['f.departure_time_slot']) }}                              as departure_slot_key,
        {{ surrogate_key(['f.arrival_time_slot']) }}                                as arrival_slot_key,

        -- derived measures / attributes -------------------------------------
        case
            when f.days_left <= 3  then '0-3 days'
            when f.days_left <= 7  then '4-7 days'
            when f.days_left <= 14 then '8-14 days'
            when f.days_left <= 30 then '15-30 days'
            else '31+ days'
        end                                                                         as booking_window,
        case
            when f.days_left <= 3  then 1
            when f.days_left <= 7  then 2
            when f.days_left <= 14 then 3
            when f.days_left <= 30 then 4
            else 5
        end                                                                         as booking_window_order,
        f.stops = 0                                                                 as is_nonstop,
        round(f.price_inr / nullif(f.duration_hours, 0), 2)                         as price_per_hour_inr,
        f.dq_warnings is not null                                                   as has_dq_warning

    from fares f

)

select * from enriched