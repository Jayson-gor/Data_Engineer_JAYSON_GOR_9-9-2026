# Bronze & Silver – Design Decisions

## Bronze ([pyspark/bronze_ingest.py](cci:7://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/pyspark/bronze_ingest.py:0:0-0:0))

| Topic | Decision | Why |
|---|---|---|
| **Schema & types** | Explicit `StructType`, **every source column `STRING`**, plus `_corrupt_record`. No `inferSchema`. | Bronze must be a faithful copy. Inferring would silently turn `6.00E-269` into a float and `0.00E+00` into `0.0`, destroying the evidence we need to repair it. Inference also costs a full extra pass over the file. |
| **Source metadata** | `_source_file`, `_source_file_hash` (SHA-256), `_source_file_size_bytes` | Provenance for every row; the hash is also the idempotency key. |
| **Ingestion metadata** | `_ingested_at`, `_batch_id` (= `YYYYMMDD_<hash12>`), `_bronze_row_id` | Lineage, reprocessing selection, deterministic dedup tie-breaking in Silver. |
| **Data preservation** | `PERMISSIVE` mode, `_corrupt_record` retained, no trimming/casting, `index` kept | Nothing is dropped or altered in Bronze; every decision about bad data is made (and recorded) in Silver where it is auditable. |
| **Partitioning** | `snapshot_date` (from filename, fallback = ingestion date) | Aligns storage with the unit of delivery, enables partition-scoped rewrites and pruning. |
| **Reruns / duplicates** | File-hash registry (append-only JSONL) + dynamic partition overwrite | Byte-identical file → skipped even under a new name. Different file for the same date → old batch `superseded`, partition replaced. Rerunning is a no-op. |
| **Schema drift** | Header validated against contract before reading; drift → `rejected_schema_drift`, nothing written, non-zero exit | Positional CSV parsing with a reordered/renamed column would silently corrupt every row. Failing loud is the only safe response. |
| **Row-count reconciliation** | Source line count vs Bronze row count stored in registry | Detects truncated reads / encoding problems at the earliest hop. |
| **Format** | Parquet + Snappy | Columnar, splittable, schema-carrying; readable by Spark, DuckDB and Redshift `COPY`/Spectrum without conversion. Delta/Iceberg were considered but add a dependency for benefits (ACID, time travel) we obtain via the registry + partition overwrite at this scale. |

## Silver ([pyspark/silver_transform.py](cci:7://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/pyspark/silver_transform.py:0:0-0:0), [pyspark/silver_rules.py](cci:7://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/pyspark/silver_rules.py:0:0-0:0))

| Topic | Decision | Why |
|---|---|---|
| **Separation of logic and I/O** | [silver_rules.py](cci:7://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/pyspark/silver_rules.py:0:0-0:0) is pure DataFrame → DataFrame; [silver_transform.py](cci:7://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/pyspark/silver_transform.py:0:0-0:0) orchestrates | Unit-testable without a lake ([tests/test_silver_rules.py](cci:7://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/tests/test_silver_rules.py:0:0-0:0)). |
| **Rule engine** | Declarative list of [Rule(name, severity, predicate, description)](cci:2://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/pyspark/silver_rules.py:93:0-98:20) | Adding a rule is one line; every rule automatically appears in `dq_results`, docs and the DQ mart. |
| **Two severities** | `error` → rejected; `warn` → kept + flagged | Not every problem should cost a row. An unrecoverable flight number on an otherwise perfect fare is still a valid price point. |
| **All reasons captured** | `rejection_reasons` lists every failed rule, not just the first | Analysts and the supplier see the full picture per record. |
| **Rejected rows keep raw values** | `flight_fares_rejected` uses original strings | You can read *why* a row failed without joining back to Bronze. |
| **Standardization** | Trim → empty→NULL → typed casts; IATA codes; display names; `stops` → int; slot ordinals; `route` | Downstream SQL never re-implements these; raw values retained alongside (`*_raw`). |
| **Flight-number repair** | Regex restore of Excel scientific notation; unrecoverable → NULL + warn; prefix cross-checked against airline IATA code | Deterministic, explainable, reversible (raw kept). |
| **Duplicate definition** | Identical on *all* business columns (`record_hash`) within a snapshot | Same itinerary with a different price is information, not noise. Survivor = lowest `_bronze_row_id`; `duplicate_count` preserved. |
| **Row identity** | `fare_record_key = sha256(record_hash ‖ snapshot_date)` | Stable across reruns → safe `unique_key` for dbt and Redshift. |
| **Outputs** | valid / rejected / dq_results / reconciliation, all partitioned by `snapshot_date` | Each is independently queryable; reconciliation proves `bronze = valid + rejected + duplicates` for every batch and fails the run otherwise. |
| **Incremental selection** | Registry status `bronze_done` → process; `--all` / `--snapshot-date` for backfills | Rerun = no-op; backfill = explicit and partition-scoped. |
| **Types chosen** | `price_inr INT`, `duration_hours DOUBLE`, `days_left INT`, `stops INT`, `snapshot_date DATE` | Smallest exact types that fit the domain; INR has no fractional component in this feed. |

## Assumptions (documented, not silently made)

1. Files are daily and named `airlines_flights_YYYYMMDD.csv`; the date is the observation date.
2. Prices are INR, tax-inclusive, single passenger.
3. Cities are the six Indian metros in the file; new cities require a reference update (and will be *rejected* until then – deliberate, so that unknown values are noticed rather than silently absorbed).
4. Non-stop domestic sectors do not exceed 6 h.
5. A re-delivered file for an existing date is a correction and fully replaces the earlier one.