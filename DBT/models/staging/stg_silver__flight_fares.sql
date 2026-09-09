-- 1:1 with the Silver valid table. Only renames / light casting happen here.
with source as (

    select * from {{ source('silver', 'flight_fares') }}

),

renamed as (

    select
        fare_record_key,
        cast(snapshot_date as date)                     as snapshot_date,

        airline_code,
        airline_name,
        flight_number,
        source_city,
        destination_city,
        route,
        departure_time_slot,
        cast(departure_slot_order as integer)           as departure_slot_order,
        arrival_time_slot,
        cast(arrival_slot_order as integer)             as arrival_slot_order,
        cast(stops as integer)                          as stops,
        cabin_class,
        cast(duration_hours as double)                  as duration_hours,
        cast(days_left as integer)                      as days_left,
        cast(price_inr as integer)                      as price_inr,

        nullif(dq_warnings, '')                         as dq_warnings,
        cast(duplicate_count as integer)                as duplicate_count,
        record_hash,

        _batch_id                                       as batch_id,
        _source_file                                    as source_file,
        cast(_ingested_at as timestamp)                 as ingested_at,
        cast(_silver_processed_at as timestamp)         as _silver_processed_at

    from source

)

select * from renamed