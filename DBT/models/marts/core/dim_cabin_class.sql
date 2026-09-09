select distinct
    cabin_class_key,
    cabin_class,
    case cabin_class when 'Business' then 1 else 2 end as class_rank
from {{ ref('int_fare_observations_enriched') }}