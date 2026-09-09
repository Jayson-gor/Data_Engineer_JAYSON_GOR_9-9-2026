"""Bronze layer: raw, immutable, append-only landing of supplier files into Parquet.

Design decisions (see documentation/bronze_layer.md for the full rationale):
  * Every source column is stored as STRING - Bronze preserves exactly what arrived,
    including values such as '6.00E-269' that we will repair in Silver.
  * Rows that cannot be parsed against the declared column count are kept in
    `_corrupt_record` rather than dropped (data preservation).
  * Ingestion metadata (`_source_file`, `_source_file_hash`, `_ingested_at`,
    `_batch_id`) is stamped on every row for lineage and reconciliation.
  * Data is partitioned by `snapshot_date` (business date from the file name).
    Writes use dynamic partition overwrite so re-running a file only rewrites its
    own partition - never the whole table.
  * A file-hash registry makes ingestion idempotent: byte-identical re-deliveries
    are skipped; a *different* file for an existing snapshot_date supersedes it.
  * The header is validated against the contract before any data is read;
    schema drift is a hard failure recorded in the registry.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

import config
from common import (
    append_registry,
    get_logger,
    get_spark,
    load_registry,
    make_batch_id,
    parse_snapshot_date,
    read_header,
    registry_has_hash,
    registry_latest_by_batch,
    sha256_file,
    utc_now_iso,
)

log = get_logger("bronze")

CORRUPT_COL = "_corrupt_record"


def bronze_schema() -> StructType:
    fields = [StructField(c, StringType(), nullable=True) for c in config.EXPECTED_COLUMNS]
    fields.append(StructField(CORRUPT_COL, StringType(), nullable=True))
    return StructType(fields)


class SchemaDriftError(RuntimeError):
    pass


def validate_header(path: Path) -> None:
    actual = read_header(path)
    expected = config.EXPECTED_COLUMNS
    if actual != expected:
        missing = [c for c in expected if c not in actual]
        extra = [c for c in actual if c not in expected]
        reordered = not missing and not extra and actual != expected
        raise SchemaDriftError(
            f"schema drift in {path.name}: missing={missing} extra={extra} reordered={reordered}"
        )


def read_source(spark: SparkSession, path: Path) -> DataFrame:
    return (
        spark.read.format("csv")
        .schema(bronze_schema())
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .option("encoding", "UTF-8")
        .option("multiLine", "false")
        .load(str(path))
    )


def add_ingestion_metadata(
    df: DataFrame, *, path: Path, file_hash: str, batch_id: str, snapshot_date: date
) -> DataFrame:
    return (
        df.withColumn("_source_file", F.lit(path.name))
        .withColumn("_source_file_hash", F.lit(file_hash))
        .withColumn("_source_file_size_bytes", F.lit(path.stat().st_size))
        .withColumn("_bronze_row_id", F.monotonically_increasing_id())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
    )


def count_source_lines(path: Path) -> int:
    with path.open("rb") as fh:
        return sum(1 for _ in fh) - 1  # minus header


def ingest_file(spark: SparkSession, path: Path, snapshot_override: str | None = None) -> dict:
    registry = load_registry()
    file_hash = sha256_file(path)

    # ---- idempotency: duplicate delivery ---------------------------------- #
    prior = registry_has_hash(registry, file_hash)
    if prior:
        msg = f"SKIP {path.name}: identical file already ingested as batch {prior['batch_id']}"
        log.info(msg)
        entry = {
            "event_at": utc_now_iso(),
            "file_name": path.name,
            "file_hash": file_hash,
            "snapshot_date": prior["snapshot_date"],
            "batch_id": prior["batch_id"],
            "status": "skipped_duplicate_file",
            "message": msg,
        }
        append_registry(entry)
        return entry

    snapshot_date = parse_snapshot_date(path, snapshot_override)
    batch_id = make_batch_id(snapshot_date, file_hash)

    # ---- schema drift ------------------------------------------------------ #
    try:
        validate_header(path)
    except SchemaDriftError as exc:
        log.error(str(exc))
        entry = {
            "event_at": utc_now_iso(),
            "file_name": path.name,
            "file_hash": file_hash,
            "snapshot_date": str(snapshot_date),
            "batch_id": batch_id,
            "status": "rejected_schema_drift",
            "message": str(exc),
        }
        append_registry(entry)
        return entry

    # ---- corrected re-delivery for an already-loaded snapshot -------------- #
    latest = registry_latest_by_batch(registry)
    superseded = [
        e for e in latest.values()
        if e.get("snapshot_date") == str(snapshot_date)
        and e.get("status") in {"bronze_done", "silver_done"}
        and e.get("file_hash") != file_hash
    ]
    for old in superseded:
        log.warning("snapshot %s already loaded from %s; new file supersedes it", snapshot_date, old["file_name"])
        append_registry({**old, "event_at": utc_now_iso(), "status": "superseded", "superseded_by": batch_id})

    # ---- read + stamp + write ---------------------------------------------- #
    log.info("INGEST %s -> snapshot_date=%s batch_id=%s", path.name, snapshot_date, batch_id)
    df = add_ingestion_metadata(
        read_source(spark, path), path=path, file_hash=file_hash, batch_id=batch_id, snapshot_date=snapshot_date
    ).cache()

    total_rows = df.count()
    corrupt_rows = df.filter(F.col(CORRUPT_COL).isNotNull()).count()
    source_lines = count_source_lines(path)

    (
        df.write.mode("overwrite")
        .partitionBy("snapshot_date")
        .parquet(str(config.BRONZE_TABLE))
    )
    df.unpersist()

    reconciled = total_rows == source_lines
    entry = {
        "event_at": utc_now_iso(),
        "file_name": path.name,
        "file_hash": file_hash,
        "file_size_bytes": path.stat().st_size,
        "snapshot_date": str(snapshot_date),
        "batch_id": batch_id,
        "status": "bronze_done",
        "source_line_count": source_lines,
        "bronze_row_count": total_rows,
        "bronze_corrupt_rows": corrupt_rows,
        "source_to_bronze_reconciled": reconciled,
        "message": "ok" if reconciled else "ROW COUNT MISMATCH between source file and bronze",
    }
    append_registry(entry)
    log.info(
        "DONE %s rows=%d corrupt=%d reconciled=%s", path.name, total_rows, corrupt_rows, reconciled
    )
    return entry


def discover_files(target: Path) -> list[Path]:
    if target.is_file():
        return [target]
    # Non-recursive on purpose: data/landing/late/ is only ingested when explicitly requested.
    return sorted(p for p in target.glob("*.csv") if p.is_file())


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Bronze ingestion")
    ap.add_argument("--input", required=True, help="CSV file or directory of CSV files")
    ap.add_argument("--snapshot-date", help="override YYYY-MM-DD (only valid with a single file)")
    args = ap.parse_args(argv)

    target = Path(args.input)
    if not target.is_absolute():
        target = config.PROJECT_ROOT / target
    files = discover_files(target)
    if not files:
        log.error("no CSV files found under %s", target)
        return 2
    if args.snapshot_date and len(files) > 1:
        log.error("--snapshot-date can only be used with a single file")
        return 2

    spark = get_spark("bronze_ingest")
    results = [ingest_file(spark, f, args.snapshot_date) for f in files]
    spark.stop()

    summary = {}
    for r in results:
        summary[r["status"]] = summary.get(r["status"], 0) + 1
    log.info("SUMMARY %s", summary)

    failed = [r for r in results if r["status"].startswith("rejected") or r.get("source_to_bronze_reconciled") is False]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())