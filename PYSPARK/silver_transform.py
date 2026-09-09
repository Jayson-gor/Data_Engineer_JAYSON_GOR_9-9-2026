"""Silver layer: Bronze -> trusted, typed, deduplicated fare observations.

Outputs (all Parquet, partitioned by snapshot_date, dynamic-overwrite):
  data/silver/flight_fares/            valid records (downstream modelling input)
  data/silver/flight_fares_rejected/   invalid records + comma-separated rejection_reasons
  data/silver/dq_results/              per-rule hit counts per batch
  data/silver/reconciliation/          bronze = valid + rejected + dedup_removed, per batch

Incremental behaviour: only batches whose registry status is `bronze_done`
(i.e. not yet promoted to Silver) are processed. Re-running is a no-op unless
--all / --snapshot-date is used, and even then the write is partition-scoped.
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import config
import silver_rules as rules
from common import append_registry, get_logger, get_spark, load_registry, registry_latest_by_batch, utc_now_iso

log = get_logger("silver")


def pending_batches(force_all: bool, snapshot_dates: list[str] | None) -> list[dict]:
    latest = registry_latest_by_batch(load_registry())
    loaded = [e for e in latest.values() if e["status"] in {"bronze_done", "silver_done"}]
    if snapshot_dates:
        return [e for e in loaded if e["snapshot_date"] in set(snapshot_dates)]
    if force_all:
        return loaded
    return [e for e in loaded if e["status"] == "bronze_done"]


def read_bronze(spark: SparkSession, snapshot_dates: list[str]) -> DataFrame:
    df = spark.read.parquet(str(config.BRONZE_TABLE))
    return df.filter(F.col("snapshot_date").cast("string").isin(snapshot_dates))


def write_partitioned(df: DataFrame, path) -> None:
    df.write.mode("overwrite").partitionBy("snapshot_date").parquet(str(path))


def reconciliation(bronze: DataFrame, valid: DataFrame, rejected: DataFrame, deduped: DataFrame) -> DataFrame:
    key = ["snapshot_date", "_batch_id"]
    b = bronze.groupBy(*key).agg(F.count("*").alias("bronze_rows"))
    v = valid.groupBy(*key).agg(F.count("*").alias("valid_rows_before_dedup"))
    r = rejected.groupBy(*key).agg(F.count("*").alias("rejected_rows"))
    d = deduped.groupBy(*key).agg(
        F.count("*").alias("silver_rows"),
        F.sum(F.col("duplicate_count") - 1).alias("duplicates_removed"),
    )
    out = b.join(v, key, "left").join(r, key, "left").join(d, key, "left").na.fill(0)
    return (
        out.withColumn(
            "is_balanced",
            F.col("bronze_rows") == F.col("silver_rows") + F.col("duplicates_removed") + F.col("rejected_rows"),
        )
        .withColumn("rejection_rate", F.round(F.col("rejected_rows") / F.col("bronze_rows"), 6))
        .withColumn("reconciled_at", F.current_timestamp())
    )


def run(spark: SparkSession, batches: list[dict]) -> list[dict]:
    dates = sorted({b["snapshot_date"] for b in batches})
    log.info("processing %d batch(es) for snapshot_date in %s", len(batches), dates)

    bronze = read_bronze(spark, dates).cache()
    scored = rules.apply_rules(rules.standardize(bronze)).cache()
    valid, rejected = rules.split_valid_rejected(scored)
    deduped = rules.deduplicate(valid).cache()

    silver_valid = rules.finalize_valid(deduped)
    silver_rejected = rules.finalize_rejected(rejected)
    dq = rules.rule_metrics(scored)
    recon = reconciliation(bronze, valid, rejected, deduped).cache()

    write_partitioned(silver_valid, config.SILVER_TABLE)
    write_partitioned(silver_rejected, config.SILVER_REJECTED_TABLE)
    write_partitioned(dq, config.SILVER_DQ_RESULTS)
    write_partitioned(recon, config.SILVER_RECONCILIATION)

    # ---- report + registry ------------------------------------------------- #
    recon_rows = {r["_batch_id"]: r.asDict() for r in recon.collect()}
    top_rules = (
        dq.filter(F.col("failed_rows") > 0)
        .orderBy(F.desc("failed_rows"))
        .select("snapshot_date", "rule_name", "severity", "failed_rows")
        .limit(15)
        .collect()
    )
    for row in top_rules:
        log.info("  rule %-32s %-5s snapshot=%s rows=%d", row.rule_name, row.severity, row.snapshot_date, row.failed_rows)

    results = []
    for b in batches:
        rc = recon_rows.get(b["batch_id"])
        if rc is None:
            log.warning("batch %s had no rows in bronze (superseded?)", b["batch_id"])
            continue
        entry = {
            **{k: b[k] for k in ("file_name", "file_hash", "snapshot_date", "batch_id")},
            "event_at": utc_now_iso(),
            "status": "silver_done",
            "bronze_rows": rc["bronze_rows"],
            "silver_rows": rc["silver_rows"],
            "rejected_rows": rc["rejected_rows"],
            "duplicates_removed": rc["duplicates_removed"],
            "is_balanced": rc["is_balanced"],
            "message": "ok" if rc["is_balanced"] else "RECONCILIATION FAILURE: bronze != silver + rejected + duplicates",
        }
        append_registry(entry)
        log.info(
            "batch %s: bronze=%d silver=%d rejected=%d dups=%d balanced=%s",
            b["batch_id"], rc["bronze_rows"], rc["silver_rows"], rc["rejected_rows"], rc["duplicates_removed"], rc["is_balanced"],
        )
        results.append(entry)

    for df in (bronze, scored, deduped, recon):
        df.unpersist()
    return results


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Silver transformation")
    ap.add_argument("--all", action="store_true", help="reprocess every loaded snapshot")
    ap.add_argument("--snapshot-date", action="append", help="YYYY-MM-DD; repeatable")
    args = ap.parse_args(argv)

    batches = pending_batches(args.all, args.snapshot_date)
    if not batches:
        log.info("nothing to do: no bronze batches pending promotion to silver")
        return 0

    spark = get_spark("silver_transform")
    results = run(spark, batches)
    spark.stop()

    unbalanced = [r for r in results if not r["is_balanced"]]
    return 1 if unbalanced else 0


if __name__ == "__main__":
    sys.exit(main())