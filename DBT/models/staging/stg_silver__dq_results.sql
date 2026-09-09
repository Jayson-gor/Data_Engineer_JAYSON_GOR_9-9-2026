select
    cast(snapshot_date as date)         as snapshot_date,
    _batch_id                           as batch_id,
    rule_name,
    severity,
    description,
    cast(failed_rows as bigint)         as failed_rows,
    cast(evaluated_rows as bigint)      as evaluated_rows,
    cast(failure_rate as double)        as failure_rate,
    cast(evaluated_at as timestamp)     as evaluated_at
from {{ source('silver', 'dq_results') }}