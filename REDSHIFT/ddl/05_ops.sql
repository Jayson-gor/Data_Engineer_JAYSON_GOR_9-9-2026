-- Load-control tables used by the orchestrator and monitoring.
CREATE TABLE IF NOT EXISTS ops.load_audit (
    snapshot_date  DATE          NOT NULL,
    batch_id       VARCHAR(32)   NOT NULL,
    table_name     VARCHAR(64)   NOT NULL,
    source_rows    BIGINT        NOT NULL,
    target_rows    BIGINT        NOT NULL,
    is_balanced    BOOLEAN       NOT NULL,
    loaded_at      TIMESTAMP     NOT NULL DEFAULT GETDATE()
)
DISTSTYLE ALL
SORTKEY (loaded_at);

-- Mirror of data/registry/ingestion_registry.jsonl so "what has been loaded"
-- is answerable from SQL.
CREATE TABLE IF NOT EXISTS ops.ingestion_registry (
    event_at        TIMESTAMP     NOT NULL,
    file_name       VARCHAR(128)  NOT NULL,
    file_hash       CHAR(64)      NOT NULL,
    snapshot_date   DATE,
    batch_id        VARCHAR(32),
    status          VARCHAR(32)   NOT NULL,
    bronze_rows     BIGINT,
    silver_rows     BIGINT,
    rejected_rows   BIGINT,
    message         VARCHAR(1024)
)
DISTSTYLE ALL
SORTKEY (event_at);