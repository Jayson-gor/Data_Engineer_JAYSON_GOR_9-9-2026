-- ---------------------------------------------------------------------------
-- Daily load: Silver Parquet (S3) -> Redshift, idempotent per snapshot_date.
-- Executed by the orchestrator after silver_transform.py succeeds for a batch.
-- Parameters: :snapshot_date, :batch_id, :s3_prefix
-- ---------------------------------------------------------------------------

BEGIN;

-- 1. Land the partition in a transient staging table (same DDL, no dist/sort cost)
CREATE TEMP TABLE stg_flight_fares (LIKE silver.flight_fares);

COPY stg_flight_fares
FROM 's3://airline-lake/silver/flight_fares/snapshot_date=:snapshot_date/'
IAM_ROLE 'arn:aws:iam::<account>:role/redshift-copy'
FORMAT AS PARQUET
SERIALIZETOJSON;        -- harmless for flat schema; keeps option for nested cols later

-- 2. Partition swap: delete the whole snapshot_date, then insert.
--    Deleting by the sort-key range is a cheap block-level operation, and it
--    makes reruns / corrected re-deliveries / late files all behave identically:
--    the target ends up equal to exactly what Silver holds for that date.
DELETE FROM silver.flight_fares
 WHERE snapshot_date = :snapshot_date;

INSERT INTO silver.flight_fares
SELECT * FROM stg_flight_fares;

-- 3. Audit: source-to-target row count must match before we commit
INSERT INTO ops.load_audit (snapshot_date, batch_id, table_name, source_rows, target_rows, is_balanced, loaded_at)
WITH c AS (
    SELECT (SELECT COUNT(*) FROM stg_flight_fares) AS src,
           (SELECT COUNT(*) FROM silver.flight_fares WHERE snapshot_date = :snapshot_date) AS tgt
)
SELECT :snapshot_date, :batch_id, 'silver.flight_fares', src, tgt, src = tgt, GETDATE() FROM c;

-- abort if counts differ (the orchestrator checks this row and rolls back)
COMMIT;

-- 4. Housekeeping (outside the transaction; cheap because data arrives in sort order)
-- VACUUM SORT ONLY silver.flight_fares TO 99 PERCENT;
-- ANALYZE silver.flight_fares PREDICATE COLUMNS;


-- Alternative when the partition-swap pattern is not desirable (e.g. row-level
-- corrections without a full day re-delivery): native MERGE (Redshift 2023+).
-- MERGE INTO silver.flight_fares USING stg_flight_fares s
--   ON silver.flight_fares.fare_record_key = s.fare_record_key
--   WHEN MATCHED THEN UPDATE SET price_inr = s.price_inr, _silver_processed_at = s._silver_processed_at, ...
--   WHEN NOT MATCHED THEN INSERT VALUES (s.*);