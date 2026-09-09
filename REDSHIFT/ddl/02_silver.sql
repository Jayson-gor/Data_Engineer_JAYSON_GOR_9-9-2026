-- ---------------------------------------------------------------------------
-- SILVER: trusted fare observations (target of the daily COPY + MERGE)
-- ---------------------------------------------------------------------------
-- DIST/SORT rationale
--   * DISTKEY(route_key-ish)  -> we distribute on a hash of the route so that the
--     most common join/aggregation (per-route pricing) is co-located. Airline has
--     only 6 values (skew) and snapshot_date is the filter column, not the join.
--   * SORTKEY(snapshot_date, ...) -> every incremental load, every dbt
--     incremental predicate and most BI filters are date-bounded, so zone maps
--     prune whole blocks. Compound (not interleaved) because we always lead with
--     the date.
--   * ENCODE AUTO lets Redshift pick AZ64/LZO per column; explicit RAW on the
--     leading sort key to avoid the classic "compressed sort key" scan penalty.
CREATE TABLE IF NOT EXISTS silver.flight_fares (
    fare_record_key        CHAR(64)       NOT NULL,        -- sha256 hex
    snapshot_date          DATE           NOT NULL,
    airline_code           CHAR(2)        NOT NULL,
    airline_name           VARCHAR(32)    NOT NULL,
    airline_raw            VARCHAR(32),
    flight_number          VARCHAR(10),                    -- NULL when unrecoverable
    flight_number_raw      VARCHAR(20),
    source_city            VARCHAR(32)    NOT NULL,
    destination_city       VARCHAR(32)    NOT NULL,
    route                  VARCHAR(65)    NOT NULL,
    departure_time_slot    VARCHAR(16)    NOT NULL,
    departure_slot_order   SMALLINT       NOT NULL,
    arrival_time_slot      VARCHAR(16)    NOT NULL,
    arrival_slot_order     SMALLINT       NOT NULL,
    stops                  SMALLINT       NOT NULL,
    stops_raw              VARCHAR(16),
    cabin_class            VARCHAR(16)    NOT NULL,
    duration_hours         DECIMAL(6,2)   NOT NULL,
    days_left              SMALLINT       NOT NULL,
    price_inr              INTEGER        NOT NULL,
    dq_warnings            VARCHAR(256),
    duplicate_count        SMALLINT       NOT NULL DEFAULT 1,
    record_hash            CHAR(64)       NOT NULL,
    _batch_id              VARCHAR(32)    NOT NULL,
    _source_file           VARCHAR(128)   NOT NULL,
    _bronze_row_id         BIGINT,
    _ingested_at           TIMESTAMP      NOT NULL,
    _silver_processed_at   TIMESTAMP      NOT NULL,
    PRIMARY KEY (fare_record_key)                          -- informational: used by the planner, not enforced
)
DISTSTYLE KEY
DISTKEY (route)
COMPOUND SORTKEY (snapshot_date, route, airline_code)
ENCODE AUTO;

ALTER TABLE silver.flight_fares ALTER COLUMN snapshot_date ENCODE RAW;

COMMENT ON TABLE silver.flight_fares IS
  'Valid, typed, deduplicated fare observations. One row per distinct fare per snapshot_date.';


CREATE TABLE IF NOT EXISTS silver.flight_fares_rejected (
    snapshot_date          DATE           NOT NULL,
    rejection_reasons      VARCHAR(512)   NOT NULL,
    dq_warnings            VARCHAR(256),
    "index"                VARCHAR(16),
    airline                VARCHAR(64),
    flight                 VARCHAR(64),
    source_city            VARCHAR(64),
    departure_time         VARCHAR(64),
    stops                  VARCHAR(64),
    arrival_time           VARCHAR(64),
    destination_city       VARCHAR(64),
    "class"                VARCHAR(64),
    duration               VARCHAR(64),
    days_left              VARCHAR(64),
    price                  VARCHAR(64),
    _corrupt_record        VARCHAR(4096),
    record_hash            CHAR(64),
    _batch_id              VARCHAR(32)    NOT NULL,
    _source_file           VARCHAR(128)   NOT NULL,
    _bronze_row_id         BIGINT,
    _ingested_at           TIMESTAMP,
    _silver_processed_at   TIMESTAMP      NOT NULL
)
DISTSTYLE EVEN                    -- small, written once, read rarely; no join pattern to optimise
COMPOUND SORTKEY (snapshot_date)
ENCODE AUTO;


CREATE TABLE IF NOT EXISTS silver.dq_results (
    snapshot_date     DATE          NOT NULL,
    _batch_id         VARCHAR(32)   NOT NULL,
    rule_name         VARCHAR(64)   NOT NULL,
    severity          VARCHAR(8)    NOT NULL,
    description       VARCHAR(256),
    failed_rows       BIGINT        NOT NULL,
    evaluated_rows    BIGINT        NOT NULL,
    failure_rate      DECIMAL(9,6),
    evaluated_at      TIMESTAMP     NOT NULL
)
DISTSTYLE ALL
SORTKEY (snapshot_date);


CREATE TABLE IF NOT EXISTS silver.reconciliation (
    snapshot_date             DATE          NOT NULL,
    _batch_id                 VARCHAR(32)   NOT NULL,
    bronze_rows               BIGINT        NOT NULL,
    valid_rows_before_dedup   BIGINT        NOT NULL,
    rejected_rows             BIGINT        NOT NULL,
    duplicates_removed        BIGINT        NOT NULL,
    silver_rows               BIGINT        NOT NULL,
    is_balanced               BOOLEAN       NOT NULL,
    rejection_rate            DECIMAL(9,6),
    reconciled_at             TIMESTAMP     NOT NULL
)
DISTSTYLE ALL
SORTKEY (snapshot_date);