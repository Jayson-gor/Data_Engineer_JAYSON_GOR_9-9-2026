# Monitoring & Observability

The pipeline already *emits* everything needed: the registry (`ingestion_registry.jsonl`
 `ops.ingestion_registry`), `silver/dq_results`, `silver/reconciliation`, dbt
`run_results.json` / source freshness, and `marts.mart_data_quality`. In production these
are shipped to CloudWatch (metrics + logs), the orchestrator (Airflow) owns task-level
alerts, and a Grafana/QuickSight board reads `mart_data_quality`.

## Key metrics

| Area | Metric | Source |
|---|---|---|
| **Freshness** | Time since last `bronze_done` for expected `snapshot_date`; dbt `source freshness` on `_silver_processed_at` | registry, dbt |
| **Volume** | `bronze_rows` per batch vs 7-day median; `silver_rows`; `fact_rows` | registry, reconciliation |
| **Quality** | `rejection_rate`; per-rule `failed_rows`; `duplicates_removed`; warn counts (`flight_number_unrecoverable`) | dq_results |
| **Integrity** | `source_to_bronze_reconciled`, `is_balanced` (bronze→silver), `silver_to_gold_balanced`, `ops.load_audit.is_balanced` | registry, reconciliation, dbt test, Redshift |
| **Schema** | count of `rejected_schema_drift` events; header diff message | registry |
| **Duplicates** | `skipped_duplicate_file` events; `superseded` events | registry |
| **Performance** | Stage durations (bronze / silver / dbt), Spark task failures, dbt model timings, Redshift `COPY` duration & `STL_LOAD_ERRORS` | logs, dbt artifacts, system tables |
| **Tests** | dbt test pass/fail count; pytest exit code | CI / orchestrator |
| **Business sanity** | Day-over-day change in `median_price_inr` per airline/cabin; `fare_share_pct` shifts | mart_airline_performance |

## Alert conditions

| Severity | Condition | Rationale |
|---|---|---|
| **P1 – page** | No file for `snapshot_date = today` by 06:00 local | SLA breach – downstream reports stale |
| **P1** | `rejected_schema_drift` event | Supplier changed the contract; nothing loaded |
| **P1** | Any reconciliation flag false (`source_to_bronze`, `is_balanced`, `silver_to_gold`, `load_audit`) | Data loss or duplication |
| **P1** | dbt `unique` / `relationships` test failure on `fct_fare_observations` | Grain violated |
| **P2 – ticket** | `rejection_rate` > 2% (or > 3σ of trailing 14 days) | Upstream quality regression |
| **P2** | `bronze_rows` deviates > 30% from 7-day median | Partial file / duplicated content |
| **P2** | `superseded` event | Correction arrived – confirm intentional |
| **P2** | Any single rule's `failed_rows` > 0 where the trailing baseline is 0 (e.g. `unknown_airline`) | New airline/city → reference data needs updating |
| **P2** | `median_price_inr` moves > 40% day-over-day for any airline × cabin | Likely currency/unit error in feed |
| **P3 – info** | `skipped_duplicate_file` | Expected; trend if frequent |
| **P3** | `flight_number_unrecoverable` count rising | Ask supplier to export as text |
| **P3** | Stage duration > 2× rolling average | Capacity planning |

## Operational runbook hooks

- Every registry event carries `batch_id`, `file_hash`, `message` → a failed run is
  traceable to the exact file.
- Rejected rows keep raw values + `rejection_reasons` → analysts can inspect without
  re-running anything.
- `mart_data_quality` places pipeline health *next to* the business data so dashboard
  consumers see "today's data reconciled" alongside the numbers.
- Rerunning any stage is safe (see [incremental_strategy.md](cci:7://file:///C:/Users/gorja/Desktop/New%20folder/Data_Engineer_JAYSON_GOR_9-9-2026/DOCUMENTATION/incremental_strategy.md:0:0-0:0)), so remediation is
  always "fix input → rerun".