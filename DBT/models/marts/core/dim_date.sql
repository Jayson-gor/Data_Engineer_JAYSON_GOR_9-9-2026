-- Calendar attributes for every snapshot_date observed so far.
with dates as (

    select distinct snapshot_date as date_day
    from {{ ref('stg_silver__flight_fares') }}

)

select
    date_day,
    {{ date_key('date_day') }}                    as date_key,
    extract(year  from date_day)                  as year,
    extract(month from date_day)                  as month,
    extract(day   from date_day)                  as day_of_month,
    extract(dow   from date_day)                  as day_of_week,
    {{ day_name('date_day') }}                    as day_name,
    extract(dow from date_day) in (0, 6)          as is_weekend
from dates