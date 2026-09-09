with airlines as (

    select distinct
        airline_key,
        airline_code,
        airline_name
    from {{ ref('int_fare_observations_enriched') }}

)

select
    airline_key,
    airline_code,
    airline_name,
    case airline_code
        when 'UK' then 'Full Service'
        when 'AI' then 'Full Service'
        else 'Low Cost'
    end as carrier_type
from airlines