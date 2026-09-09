with routes as (

    select distinct
        route_key,
        source_city,
        destination_city,
        route
    from {{ ref('int_fare_observations_enriched') }}

)

select
    route_key,
    source_city,
    destination_city,
    route,
    -- direction-agnostic pair so O&D analysis can group both directions
    case when source_city < destination_city
         then source_city || '<>' || destination_city
         else destination_city || '<>' || source_city
    end as city_pair
from routes