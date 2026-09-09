# Incremental Processing Strategy

**Assumption:** one file per day named `airlines_flights_YYYYMMDD.csv`; the date in the
name is the business date (`snapshot_date`) on which the fares were observed.

## Unit of work = `snapshot_date` partition

Every layer is partitioned by `snapshot_date` and every write is scoped to the
partitions being processed. Nothing ever rewrites history it was not asked to touch.

| Layer | Mechanism |
|---|---|
| Bronze | Spark `partitionOverwriteMode=dynamic` → only the file's own `snapshot_date` folder is replaced. |
| Silver | Registry-driven: process batches in status `bronze_done`; write the same partitions in valid / rejected / dq / reconciliation. |
| Gold (dbt) | `incremental_strategy='delete+insert'`, `unique_key='snapshot_date'`; the model selects *all* rows for "affected snapshots" and swaps those partitions. |
| Redshift | `DELETE WHERE snapshot_date = :d; INSERT ...` in one transaction (see `redshift/load/`). |

## How each scenario is handled

| Scenario | Bronze | Silver | Gold |
|---|---|---|---|
| **New daily file** | New partition written, registry `bronze_done` | Picked up as pending, promoted, `silver_done` | `_silver_processed_at` > watermark → partition inserted |
| **Changed records** (same itinerary, new price on a new day) | New rows in the new day's partition | New `fare_record_key` (hash includes snapshot_date) | New facts; marts recomputed for that day only |
| **Corrected re-delivery** (same date, different content) | Old batch → `superseded`; partition overwritten | Partition re-derived from the new bronze content | Partition replaced (delete+insert) – old rows disappear, no residue |
| **Duplicate file delivery** (byte-identical, any name) | Hash match → `skipped_duplicate_file`, **no write** | Nothing pending | No-op |
| **Pipeline rerun** (same inputs) | All hashes known → skipped | No `bronze_done` batches → "nothing to do" | Zero affected snapshots → zero rows inserted, zero deleted |
| **Late-arriving file** (old date arrives today) | Lands in its own old partition | Promoted like any batch | Its `_silver_processed_at` is *newer* than the watermark even though the date is old → partition replaced. No lookback window needed. |
| **Silver rule change / backfill** | untouched | `silver_transform.py --all` (or `--snapshot-date`) rewrites chosen partitions | Their `_silver_processed_at` moves forward → dbt replaces exactly those partitions |
| **Model definition change** | – | – | `dbt build --full-refresh` (only when SQL changes, never for data) |

## Why no double counting is possible

1. **Bronze** cannot hold the same file twice (hash registry) and cannot hold two
   versions of the same `snapshot_date` (dynamic overwrite).
2. **Silver** row identity is `fare_record_key = sha256(business columns ‖ snapshot_date)`;
   partitions are derived deterministically from Bronze and overwritten whole.
3. **Gold** never appends: it deletes every `snapshot_date` it is about to insert.
   Reprocessing a partition N times yields the same rows once.
4. **Redshift** load uses the same delete/insert-by-date pattern with an audit row that
   must balance before commit.

Tests proving this: [test_rerun_same_file_is_skipped_and_does_not_double_count](cci:1://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/tests/test_bronze_ingest.py:27:0-33:43),
[test_corrected_file_for_same_snapshot_supersedes_old_partition](cci:1://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/tests/test_bronze_ingest.py:44:0-53:35),
[test_late_arriving_file_lands_in_its_own_partition](cci:1://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/tests/test_bronze_ingest.py:56:0-63:26), dbt
`assert_fact_matches_silver_rowcount`, and the full `run_pipeline.sh full` executed
twice in a row (second run is a no-op).

## Demonstration built into the repo

[prepare_landing.py](cci:7://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/pyspark/prepare_landing.py:0:0-0:0) creates: an initial full file (2026-09-01), a day-2 delta with
injected faults, a **byte-identical re-delivery** of day 2, a clean day-3 delta and a
**late** file dated 2026-08-31 in `data/landing/late/` (deliberately outside the
auto-ingest glob). After `full`, run:

```bash
bash scripts/run_pipeline.sh daily data/landing/late/airlines_flights_20260831.csv