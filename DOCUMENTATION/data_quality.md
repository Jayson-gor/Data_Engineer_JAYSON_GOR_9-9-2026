# Data Quality & Reconciliation

## 1. Profiling findings (supplied file, 300,153 rows)

| Finding | Evidence | Decision |
|---|---|---|
| **Excel-corrupted flight numbers** | 4,705 IndiGo rows have `6E-269` stored as `6.00E-269`; 51 rows are `0.00E+00` (originally `6E-0xx`, digits lost). Only IndiGo is affected because `6E` *looks* like scientific notation. | Deterministic regex repair `^(\d)\.00E-(\d+)$ → {d}E-{n}` (warn `flight_number_repaired`). `0.00E+00` is unrecoverable → keep the row, `flight_number = NULL`, warn `flight_number_unrecoverable`. Rejecting would lose 51 valid fare prices for a non-key attribute. |
| **No nulls, no exact duplicates** | `isna().sum()` all zero; `duplicated()` = 0 | Rules still implemented – the daily feed will not be this clean. Faults are injected into the simulated day-2 file to prove they work. |
| **64,392 rows share a natural key** (airline, flight, route, slots, class, days_left) and 19,088 of those groups have **different prices** | Same itinerary quoted at several prices on the same day (fare buckets / channels) | This is a *fare-observation* dataset, not a schedule. Exact duplicates (all business columns identical) are collapsed; price variants are **kept** as separate facts. Grain documented accordingly. |
| `index` column | Contiguous 0..N-1, regenerated per file | Supplier surrogate, not a business key → excluded from `record_hash`, kept in Bronze/rejected for traceability. |
| Categorical domains are closed | 6 airlines, 6 cities, 6 slots, 3 stop buckets, 2 classes | Encoded as reference sets in [pyspark/config.py](cci:7://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/pyspark/config.py:0:0-0:0); also `accepted_values` dbt tests. |
| Numeric ranges plausible | duration 0.83–49.83 h, days_left 1–49, price 1,105–123,071 INR; all non-stop ≤ 3.6 h | Range rules set with headroom (e.g. non-stop ≤ 6 h) so real outliers are caught without false positives. |
| No date column | – | `snapshot_date` derived from the file name (`YYYYMMDD`), fallback = ingestion date. Documented assumption. |
| Inconsistent naming | `Air_India`, `GO_FIRST`, `Early_Morning` | Raw kept (`airline_raw`); display names and IATA codes added. |

## 2. Rule catalogue (Silver, [pyspark/silver_rules.py](cci:7://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/pyspark/silver_rules.py:0:0-0:0))

Every rule is a named, documented [Rule(name, severity, condition, description)](cci:2://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/pyspark/silver_rules.py:93:0-98:20).
Results per rule per batch are written to `silver/dq_results` and surfaced in
`marts.mart_data_quality`.

| Category | Rules | Severity |
|---|---|---|
| Structural | `corrupt_record` | error |
| Completeness | `missing_<col>` for all 11 business columns | error |
| Domain / referential | `unknown_airline`, `unknown_source_city`, `unknown_destination_city`, `unknown_departure_time`, `unknown_arrival_time`, `unknown_stops`, `unknown_class` | error |
| Type / range | `duration_not_numeric`, `duration_out_of_range`, `days_left_not_numeric`, `days_left_out_of_range`, `price_not_numeric`, `price_out_of_range` | error |
| Business logic | `same_source_destination`, `nonstop_duration_implausible` | error |
| Repair evidence | `flight_number_repaired`, `flight_number_unrecoverable`, `flight_prefix_airline_mismatch` | warn |

- **error** → row goes to `flight_fares_rejected` with **all** failing rule names in
  `rejection_reasons` (comma-separated) and its original raw values.
- **warn** → row is kept; names recorded in `dq_warnings`.

## 3. Duplicate handling

| Level | Mechanism |
|---|---|
| Duplicate **file** | SHA-256 of the file checked against the registry → skipped, logged as `skipped_duplicate_file`. |
| Duplicate **row within a file** | `record_hash` = SHA-256 of all business columns. Copies collapsed; survivor keeps `duplicate_count`. |
| Same **snapshot_date** re-delivered with different content | Treated as a correction: old batch marked `superseded`, partition overwritten. |
| Same itinerary, different price | Not a duplicate – distinct fare observation. |

## 4. Schema drift

The CSV header is compared to the contract (`EXPECTED_COLUMNS`) **before** any data is
read. Missing, extra or re-ordered columns → `rejected_schema_drift` in the registry,
nothing written, non-zero exit. (Re-ordering is treated as drift because positional
CSV parsing would silently mis-map values.)

## 5. Reconciliation (source → target)

| Hop | Check | Where |
|---|---|---|
| File → Bronze | `source_line_count == bronze_row_count` | registry entry, `source_to_bronze_reconciled` |
| Bronze → Silver | `bronze_rows == silver_rows + rejected_rows + duplicates_removed` | `silver/reconciliation.is_balanced`; pipeline exits 1 if false |
| Silver → Gold | per-snapshot row counts equal | dbt singular test `assert_fact_matches_silver_rowcount`, `mart_data_quality.silver_to_gold_balanced` |
| Silver → Redshift | staging vs target count per snapshot | `ops.load_audit.is_balanced` |

## 6. Automated tests

- [tests/test_silver_rules.py](cci:7://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/tests/test_silver_rules.py:0:0-0:0) – unit tests for repair, every injected fault class, dedup semantics, metric coverage.
- [tests/test_bronze_ingest.py](cci:7://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/tests/test_bronze_ingest.py:0:0-0:0) – idempotency, duplicate file, supersede, late arrival, schema drift, short-row preservation.
- [tests/test_pipeline_integration.py](cci:7://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/tests/test_pipeline_integration.py:0:0-0:0) – runs against the built lake: reconciliation, injected-fault counts, no invalid values in Silver, Gold = Silver.
- `dbt build` – 48 schema tests (`unique`, `not_null`, `accepted_values`, `relationships`, custom `positive_value`, `value_between`, `unique_combination`) + 3 singular tests.