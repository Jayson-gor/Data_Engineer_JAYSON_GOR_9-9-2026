select
    cast(snapshot_date as date)             as snapshot_date,
    _batch_id                               as batch_id,
    cast(bronze_rows as bigint)             as bronze_rows,
    cast(valid_rows_before_dedup as bigint) as valid_rows_before_dedup,
    cast(rejected_rows as bigint)           as rejected_rows,
    cast(duplicates_removed as bigint)      as duplicates_removed,
    cast(silver_rows as bigint)             as silver_rows,
    is_balanced,
    cast(rejection_rate as double)          as rejection_rate,
    cast(reconciled_at as timestamp)        as reconciled_at
from {{ source('silver', 'reconciliation') }}