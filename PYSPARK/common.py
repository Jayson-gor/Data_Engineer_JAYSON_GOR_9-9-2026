"""Shared helpers: Spark session, hashing, snapshot-date parsing, ingestion registry."""
from __future__ import annotations

import hashlib
import json
import logging
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

import config

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


# --------------------------------------------------------------------------- #
# Spark
# --------------------------------------------------------------------------- #
def get_spark(app_name: str) -> "SparkSession":
    from pyspark.sql import SparkSession  # lazy: lets pandas-only tooling import this module

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        # Only replace the partitions we actually write -> safe reruns.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# --------------------------------------------------------------------------- #
# File helpers
# --------------------------------------------------------------------------- #
def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def read_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig") as fh:
        first = fh.readline().rstrip("\r\n")
    return [c.strip() for c in first.split(",")]


def parse_snapshot_date(path: Path, override: str | None = None) -> date:
    """Snapshot date = business date the fares were observed.

    Priority: explicit override > YYYYMMDD in filename > today (UTC).
    """
    if override:
        return datetime.strptime(override, "%Y-%m-%d").date()
    m = re.search(config.SNAPSHOT_DATE_REGEX, path.stem)
    if m:
        return datetime.strptime(m.group(1), config.SNAPSHOT_DATE_FORMAT).date()
    return datetime.now(timezone.utc).date()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def make_batch_id(snapshot_date: date, file_hash: str) -> str:
    return f"{snapshot_date:%Y%m%d}_{file_hash[:12]}"


# --------------------------------------------------------------------------- #
# Ingestion registry (append-only JSONL; tiny, so no Spark needed)
# --------------------------------------------------------------------------- #
def load_registry() -> list[dict]:
    if not config.REGISTRY_FILE.exists():
        return []
    with config.REGISTRY_FILE.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def append_registry(entry: dict) -> None:
    config.REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    with config.REGISTRY_FILE.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, default=str) + "\n")


def registry_latest_by_batch(entries: Iterable[dict]) -> dict[str, dict]:
    """Collapse the append-only log to the latest status per batch_id."""
    latest: dict[str, dict] = {}
    for e in entries:
        latest[e["batch_id"]] = e
    return latest


def registry_has_hash(entries: Iterable[dict], file_hash: str) -> dict | None:
    for e in entries:
        if e.get("file_hash") == file_hash and e.get("status") in {"bronze_done", "silver_done"}:
            return e
    return None