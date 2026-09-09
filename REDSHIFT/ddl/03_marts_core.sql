-- ---------------------------------------------------------------------------
-- MARTS / CORE: star schema (dbt materializes these; DDL shown for design review)
-- ---------------------------------------------------------------------------

-- Dimensions are tiny (6-36 rows) -> DISTSTYLE ALL puts a full copy on every
-- node so fact joins never shuffle.
CREATE TABLE IF NOT EXISTS marts.dim_airline (
    airline_key    CHAR(32)     NOT NULL,   -- md5 hex
    airline_code   CHAR(2)      NOT NULL,
    airline_name   VARCHAR(32)  NOT NULL,
    carrier_type   VARCHAR(16)  NOT NULL,
    PRIMARY KEY (airline_key)
) DISTSTYLE ALL SORTKEY (airline_key);

CREATE TABLE IF NOT EXISTS marts.dim_route (
    route_key         CHAR(32)     NOT NULL,
    source_city       VARCHAR(32)  NOT NULL,
    destination_city  VARCHAR(32)  NOT NULL,
    route             VARCHAR(65)  NOT NULL,
    city_pair         VARCHAR(66)  NOT NULL,
    PRIMARY KEY (route_key)
) DISTSTYLE ALL SORTKEY (route_key);

CREATE TABLE IF NOT EXISTS marts.dim_time_slot (
    time_slot_key        CHAR(32)     NOT NULL,
    time_slot            VARCHAR(16)  NOT NULL,
    slot_order           SMALLINT     NOT NULL,
    time_slot_label      VARCHAR(16)  NOT NULL,
    approx_time_window   VARCHAR(11),
    PRIMARY KEY (time_slot_key)
) DISTSTYLE ALL SORTKEY (time_slot_key);

CREATE TABLE IF NOT EXISTS marts.dim_cabin_class (
    cabin_class_key  CHAR(32)     NOT NULL,
    cabin_class      VARCHAR(16)  NOT NULL,
    class_rank       SMALLINT     NOT NULL,
    PRIMARY KEY (cabin_class_key)
) DISTSTYLE ALL SORTKEY (cabin_class_key);

CREATE TABLE IF NOT EXISTS marts.dim_date (
    date_day      DATE         NOT NULL,
    date_key      INTEGER      NOT NULL,
    year          SMALLINT     NOT NULL,
    month         SMALLINT     NOT NULL,
    day_of_month  SMALLINT     NOT NULL,
    day_of_week   SMALLINT     NOT NULL,
    day_name      VARCHAR(9)   NOT NULL,
    is_weekend    BOOLEAN      NOT NULL,
    PRIMARY KEY (date_day)
) DISTSTYLE ALL SORTKEY (date_day);


-- FACT ---------------------------------------------------------------------
-- Grain: one row per distinct fare observation per snapshot_date.
-- ~300k rows/day today -> ~110M rows/year. Design choices for that growth:
--   * DISTKEY(route_key): co-locates fact rows with the dominant GROUP BY /
--     JOIN attribute; 30 routes gives even enough distribution across slices
--     (airline_code with 6 values would skew).
--   * COMPOUND SORTKEY(snapshot_date, route_key): loads arrive in date order so
--     the table stays naturally sorted -> VACUUM is cheap; date-range predicates
--     from dbt incremental + BI prune ~all blocks.
--   * Surrogate keys as CHAR(32) md5. On a real deployment we would switch to
--     BIGINT identity keys to shrink the fact (32 bytes -> 8 bytes per key).
CREATE TABLE IF NOT EXISTS marts.fct_fare_observations (
    fare_record_key        CHAR(64)      NOT NULL,
    snapshot_date          DATE          NOT NULL,
    snapshot_date_key      INTEGER       NOT NULL,
    airline_key            CHAR(32)      NOT NULL REFERENCES marts.dim_airline (airline_key),
    route_key              CHAR(32)      NOT NULL REFERENCES marts.dim_route (route_key),
    cabin_class_key        CHAR(32)      NOT NULL REFERENCES marts.dim_cabin_class (cabin_class_key),
    departure_slot_key     CHAR(32)      REFERENCES marts.dim_time_slot (time_slot_key),
    arrival_slot_key       CHAR(32)      REFERENCES marts.dim_time_slot (time_slot_key),
    flight_number          VARCHAR(10),
    stops                  SMALLINT      NOT NULL,
    is_nonstop             BOOLEAN       NOT NULL,
    days_left              SMALLINT      NOT NULL,
    booking_window         VARCHAR(12)   NOT NULL,
    booking_window_order   SMALLINT      NOT NULL,
    price_inr              INTEGER       NOT NULL,
    duration_hours         DECIMAL(6,2)  NOT NULL,
    price_per_hour_inr     DECIMAL(12,2),
    duplicate_count        SMALLINT      NOT NULL,
    has_dq_warning         BOOLEAN       NOT NULL,
    dq_warnings            VARCHAR(256),
    batch_id               VARCHAR(32)   NOT NULL,
    source_file            VARCHAR(128)  NOT NULL,
    ingested_at            TIMESTAMP     NOT NULL,
    _silver_processed_at   TIMESTAMP     NOT NULL,
    PRIMARY KEY (fare_record_key)
)
DISTSTYLE KEY
DISTKEY (route_key)
COMPOUND SORTKEY (snapshot_date, route_key)
ENCODE AUTO;

ALTER TABLE marts.fct_fare_observations ALTER COLUMN snapshot_date ENCODE RAW;