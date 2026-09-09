-- Source-to-target reconciliation: the fact must hold exactly the Silver valid rows
-- for every snapshot_date (no loss, no double counting after reruns).
with silver as (
    select snapshot_date, count(*) as silver_rows
    from {{ ref('stg_silver__flight_fares') }}
    group by 1
),
gold as (
    select snapshot_date, count(*) as gold_rows
    from {{ ref('fct_fare_observations') }}
    group by 1
)
select
    coalesce(s.snapshot_date, g.snapshot_date) as snapshot_date,
    s.silver_rows,
    g.gold_rows
from silver s
full outer join gold g on g.snapshot_date = s.snapshot_date
where coalesce(s.silver_rows, -1) <> coalesce(g.gold_rows, -1)