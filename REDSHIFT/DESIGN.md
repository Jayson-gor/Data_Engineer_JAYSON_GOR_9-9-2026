# Amazon Redshift Design

No live cluster was used; this folder contains the DDL (`ddl/`), the daily load
pattern (`load/`) and the reasoning below. The dbt project runs unchanged against
Redshift by setting `DBT_TARGET=redshift` (adapter-specific SQL is isolated in
[dbt/macros/cross_db.sql](cci:7://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/dbt/macros/cross_db.sql:0:0-0:0)).

## Layer mapping

| Medallion | Where it lives                         | Why                                                                                     |
|-----------|----------------------------------------|-----------------------------------------------------------------------------------------|
| Bronze    | S3 Parquet, exposed via **Spectrum**   | Raw strings, append-only, rarely queried. Paying Redshift storage for it is wasteful.   |
| Silver    | `silver.*` tables, loaded by `COPY`    | Typed, deduplicated, the only data dbt reads. Needs fast date-bounded scans.            |
| Gold      | `marts.*` tables, built by dbt         | Star schema + pre-aggregated marts for BI.                                              |
| Ops       | `ops.*`                                | Load audit and registry mirror for monitoring.                                          |

## Key decisions

### Distribution
- **Fact and Silver: `DISTKEY(route_key)`.** The dominant join/aggregation is by route
  (30 distinct values → spreads reasonably across slices). `airline_code` has only 6
  values (heavy skew, Vistara = 43% of rows) and `snapshot_date` is a *filter*, not a
  join key, so both were rejected.
- **Dimensions: `DISTSTYLE ALL`.** They are 2–36 rows; a full copy on each node
  eliminates redistribution on every fact join.
- **Analytics marts: `DISTSTYLE ALL`.** A few thousand rows per day; BI tools join them
  to dimensions freely.

### Sort keys
- **`COMPOUND SORTKEY(snapshot_date, route_key[, airline_code])`** everywhere the data
  is date-partitioned. Every incremental load, every dbt `is_incremental()` predicate
  and most dashboards filter by date first, so zone maps prune nearly all blocks.
  Compound rather than interleaved because we always lead with the date and data
  arrives in date order (keeps the table naturally sorted → `VACUUM` is cheap).
- Leading sort-key column set to `ENCODE RAW` (Redshift best practice: a compressed
  leading sort key inflates the number of blocks scanned for range restrictions).

### Encoding
- `ENCODE AUTO` – Redshift picks AZ64 for numerics/dates and LZO/ZSTD for varchars and
  re-evaluates as the table grows.

### Keys and constraints
- `PRIMARY KEY` / `REFERENCES` are declared but **not enforced** by Redshift; they are
  planner hints. Uniqueness is guaranteed upstream (Silver dedup + dbt `unique` tests).
- Surrogate keys are `md5` `CHAR(32)` for portability with DuckDB. At scale we would
  move to `BIGINT IDENTITY` keys (4× smaller, faster hash joins).

## Load pattern (idempotent)

[load/01_load_silver_from_s3.sql](cci:7://file:///C:/Users/gorja/Downloads/amptitude_test_kq_07_09_2026/amptitude_test_kq_07_09_2026/redshift/load/01_load_silver_from_s3.sql:0:0-0:0):

1. `COPY` the single `snapshot_date=` prefix from S3 into a `TEMP` table (parallel,
   Parquet, one file per slice ideally – Spark's output partition count is tuned for
   this).
2. `DELETE ... WHERE snapshot_date = :d` then `INSERT ... SELECT` inside one
   transaction → **partition swap**. Rerun, corrected re-delivery and late-arriving
   file all reduce to the same operation; no double counting is possible.
3. Write an `ops.load_audit` row with source vs target counts; the orchestrator
   fails the task if `is_balanced = false`.
4. `VACUUM SORT ONLY` / `ANALYZE PREDICATE COLUMNS` outside the transaction.

`MERGE` (GA since 2023) is shown as the alternative for row-level upserts.

## Behaviour as volume grows

| Concern                                     | Mitigation                                                                                                                                   |
|---------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| 300k rows/day → ~110M rows/year in the fact | Date-leading sort key keeps daily loads and queries touching ~1/365 of blocks. RA3 nodes decouple storage; managed storage scales to PB.     |
| More airlines / routes                      | `DISTKEY(route_key)` cardinality rises → *better* distribution. Dimension `DISTSTYLE ALL` stays valid until tens of MB.                       |
| Historical rebuilds                         | dbt full-refresh only when a model definition changes; otherwise partition-scoped. Bronze in S3 means a rebuild never needs the supplier.    |
| Concurrency / BI load                       | Analytics marts are pre-aggregated; WLM queue for dbt vs BI; Concurrency Scaling for bursts.                                                 |
| Vacuum cost                                 | Data arrives in sort order (date) → sort-only vacuum is near free; deletes are date-range → `VACUUM DELETE ONLY` weekly.                     |
| Schema evolution                            | dbt `on_schema_change: append_new_columns`; Spectrum handles Bronze drift without DDL.                                                       |
| Cost                                        | Bronze on S3 (cheap), Silver in Redshift, oldest Silver partitions can be `UNLOAD`ed to S3 and re-attached via Spectrum after N years.        |