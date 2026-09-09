-- Shared by departure and arrival roles (role-playing dimension).
with slots as (

    select distinct departure_time_slot as time_slot, departure_slot_order as slot_order
    from {{ ref('int_fare_observations_enriched') }}
    union
    select distinct arrival_time_slot, arrival_slot_order
    from {{ ref('int_fare_observations_enriched') }}

)

select
    {{ surrogate_key(['time_slot']) }} as time_slot_key,
    time_slot,
    slot_order,
    replace(time_slot, '_', ' ')       as time_slot_label,
    case time_slot
        when 'Early_Morning' then '04:00-08:00'
        when 'Morning'       then '08:00-12:00'
        when 'Afternoon'     then '12:00-16:00'
        when 'Evening'       then '16:00-20:00'
        when 'Night'         then '20:00-24:00'
        when 'Late_Night'    then '00:00-04:00'
    end                                as approx_time_window
from slots