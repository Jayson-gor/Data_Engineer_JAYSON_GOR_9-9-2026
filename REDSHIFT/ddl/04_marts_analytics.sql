-- ---------------------------------------------------------------------------
-- MARTS / ANALYTICS: pre-aggregated business marts (dbt incremental)
-- All are small (hundreds to a few thousand rows per snapshot_date) so we
-- favour DISTSTYLE ALL / EVEN and a date-leading sort key for dashboard filters.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS marts.mart_route_pricing (
    snapshot_date            DATE          NOT NULL,
    route                    VARCHAR(65)   NOT NULL,
    source_city              VARCHAR(32)   NOT NULL,
    destination_city         VARCHAR(32)   NOT NULL,
    airline_code             CHAR(2)       NOT NULL,
    airline_name             VARCHAR(32)   NOT NULL,
    cabin_class              VARCHAR(16)   NOT NULL,
    stops                    SMALLINT      NOT NULL,
    fare_count               BIGINT        NOT NULL,
    distinct_flights         BIGINT        NOT NULL,
    min_price_inr            INTEGER,
    median_price_inr         DECIMAL(12,2),
    avg_price_inr            DECIMAL(12,2),
    max_price_inr            INTEGER,
    price_stddev_inr         DECIMAL(12,2),
    avg_duration_hours       DECIMAL(6,2),
    avg_price_per_hour_inr   DECIMAL(12,2),
    price_rank_on_route      SMALLINT,
    premium_vs_cheapest_inr  DECIMAL(12,2),
    _silver_processed_at     TIMESTAMP     NOT NULL
)
DISTSTYLE ALL
COMPOUND SORTKEY (snapshot_date, route, cabin_class);


CREATE TABLE IF NOT EXISTS marts.mart_booking_lead_time (
    snapshot_date              DATE          NOT NULL,
    airline_code               CHAR(2)       NOT NULL,
    airline_name               VARCHAR(32)   NOT NULL,
    cabin_class                VARCHAR(16)   NOT NULL,
    booking_window             VARCHAR(12)   NOT NULL,
    booking_window_order       SMALLINT      NOT NULL,
    fare_count                 BIGINT        NOT NULL,
    min_days_left              SMALLINT,
    max_days_left              SMALLINT,
    median_price_inr           DECIMAL(12,2),
    avg_price_inr              DECIMAL(12,2),
    min_price_inr              INTEGER,
    max_price_inr              INTEGER,
    early_booking_median_inr   DECIMAL(12,2),
    premium_vs_early_inr       DECIMAL(12,2),
    premium_vs_early_pct       DECIMAL(9,2),
    _silver_processed_at       TIMESTAMP     NOT NULL
)
DISTSTYLE ALL
COMPOUND SORTKEY (snapshot_date, airline_code, cabin_class);


CREATE TABLE IF NOT EXISTS marts.mart_airline_performance (
    snapshot_date            DATE          NOT NULL,
    airline_code             CHAR(2)       NOT NULL,
    airline_name             VARCHAR(32)   NOT NULL,
    carrier_type             VARCHAR(16)   NOT NULL,
    cabin_class              VARCHAR(16)   NOT NULL,
    fare_count               BIGINT        NOT NULL,
    routes_served            SMALLINT,
    distinct_flights         INTEGER,
    nonstop_pct              DECIMAL(5,2),
    avg_stops                DECIMAL(5,3),
    avg_duration_hours       DECIMAL(6,2),
    median_price_inr         DECIMAL(12,2),
    avg_price_inr            DECIMAL(12,2),
    avg_price_per_hour_inr   DECIMAL(12,2),
    dq_warning_pct           DECIMAL(5,2),
    fare_share_pct           DECIMAL(5,2),
    price_rank               SMALLINT,
    _silver_processed_at     TIMESTAMP     NOT NULL
)
DISTSTYLE ALL
COMPOUND SORTKEY (snapshot_date, cabin_class);


CREATE TABLE IF NOT EXISTS marts.mart_time_slot_demand (
    snapshot_date           DATE          NOT NULL,
    route                   VARCHAR(65)   NOT NULL,
    departure_time_slot     VARCHAR(16)   NOT NULL,
    departure_slot_order    SMALLINT      NOT NULL,
    approx_time_window      VARCHAR(11),
    cabin_class             VARCHAR(16)   NOT NULL,
    fare_count              BIGINT        NOT NULL,
    airlines_operating      SMALLINT,
    distinct_flights        INTEGER,
    median_price_inr        DECIMAL(12,2),
    avg_price_inr           DECIMAL(12,2),
    nonstop_pct             DECIMAL(5,2),
    slot_share_pct          DECIMAL(5,2),
    _silver_processed_at    TIMESTAMP     NOT NULL
)
DISTSTYLE ALL
COMPOUND SORTKEY (snapshot_date, route, departure_slot_order);


CREATE TABLE IF NOT EXISTS marts.mart_data_quality (
    snapshot_date               DATE          NOT NULL,
    batch_id                    VARCHAR(32)   NOT NULL,
    rule_name                   VARCHAR(64)   NOT NULL,
    severity                    VARCHAR(8)    NOT NULL,
    description                 VARCHAR(256),
    failed_rows                 BIGINT        NOT NULL,
    evaluated_rows              BIGINT        NOT NULL,
    failure_rate                DECIMAL(9,6),
    bronze_rows                 BIGINT,
    rejected_rows               BIGINT,
    duplicates_removed          BIGINT,
    silver_rows                 BIGINT,
    fact_rows                   BIGINT,
    bronze_to_silver_balanced   BOOLEAN,
    silver_to_gold_balanced     BOOLEAN,
    rejection_rate              DECIMAL(9,6),
    evaluated_at                TIMESTAMP     NOT NULL
)
DISTSTYLE ALL
COMPOUND SORTKEY (snapshot_date, batch_id);