-- Business rule: non-stop Indian domestic sectors cannot exceed 6 hours.
select fare_record_key, flight_number, duration_hours
from {{ ref('fct_fare_observations') }}
where is_nonstop and duration_hours > 6