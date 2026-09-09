-- Business rule: a fare can never be for a flight from a city to itself.
select route_key, source_city, destination_city
from {{ ref('dim_route') }}
where source_city = destination_city