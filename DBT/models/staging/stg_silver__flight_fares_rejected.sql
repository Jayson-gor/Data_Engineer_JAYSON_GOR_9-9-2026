with source as (

    select * from {{ source('silver', 'flight_fares_rejected') }}

)

select
    cast(snapshot_date as date)             as snapshot_date,
    rejection_reasons,
    nullif(dq_warnings, '')                 as dq_warnings,
    airline,
    flight,
    source_city,
    destination_city,
    departure_time,
    arrival_time,
    stops,
    class                                   as cabin_class,
    duration,
    days_left,
    price,
    _corrupt_record                         as corrupt_record,
    record_hash,
    _batch_id                               as batch_id,
    _source_file                            as source_file,
    cast(_silver_processed_at as timestamp) as _silver_processed_at
from source