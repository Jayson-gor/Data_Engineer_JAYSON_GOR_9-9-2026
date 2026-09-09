"""Pure PySpark transformations for the Silver layer (no I/O -> unit-testable).

Pipeline:  standardize() -> apply_rules() -> split_valid_rejected() -> deduplicate()

Rule severities
  error : record is rejected and lands in flight_fares_rejected with the reasons
  warn  : record is kept; the reason is recorded in `dq_warnings`
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

import config

# --------------------------------------------------------------------------- #
# 1. Standardization
# --------------------------------------------------------------------------- #
# Excel turned flight numbers such as 6E-269 into scientific notation (6.00E-269).
# 6E-0xx collapsed to 0.00E+00 which is unrecoverable.
_SCI_NOTATION = r"^(\d)\.00E-(\d+)$"
_FLIGHT_PATTERN = r"^[A-Z0-9]{2}-\d{1,4}$"


def _trim_to_null(col: str) -> Column:
    c = F.trim(F.col(col))
    return F.when(c == "", None).otherwise(c)


def _map_expr(mapping: dict[str, str | int]) -> Column:
    return F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])


def repair_flight_number(col: Column) -> Column:
    """Deterministic repair of Excel scientific-notation corruption."""
    repaired = F.when(
        col.rlike(_SCI_NOTATION),
        F.concat(F.regexp_extract(col, _SCI_NOTATION, 1), F.lit("E-"), F.regexp_extract(col, _SCI_NOTATION, 2)),
    ).otherwise(col)
    # Anything still not matching the carrier-code pattern is unrecoverable -> NULL
    return F.when(repaired.rlike(_FLIGHT_PATTERN), repaired).otherwise(F.lit(None))


def standardize(bronze: DataFrame) -> DataFrame:
    """Cast, trim, normalize and derive - keeps raw copies of anything we changed."""
    df = bronze
    for c in config.BUSINESS_COLUMNS:
        df = df.withColumn(c, _trim_to_null(c))

    airline_code = _map_expr(config.AIRLINE_IATA_PREFIX)
    airline_name = _map_expr(config.AIRLINE_DISPLAY_NAME)
    slot_order = _map_expr(config.TIME_SLOT_ORDER)
    stops_int = _map_expr(config.STOPS_TO_INT)

    return (
        df.withColumn("airline_raw", F.col("airline"))
        .withColumn("airline_code", airline_code[F.col("airline")])
        .withColumn("airline_name", F.coalesce(airline_name[F.col("airline")], F.col("airline")))
        .withColumn("flight_number_raw", F.col("flight"))
        .withColumn("flight_number", repair_flight_number(F.col("flight")))
        .withColumn("flight_prefix", F.split(F.col("flight_number"), "-").getItem(0))
        .withColumn("departure_time_slot", F.col("departure_time"))
        .withColumn("departure_slot_order", slot_order[F.col("departure_time")].cast(IntegerType()))
        .withColumn("arrival_time_slot", F.col("arrival_time"))
        .withColumn("arrival_slot_order", slot_order[F.col("arrival_time")].cast(IntegerType()))
        .withColumn("stops_raw", F.col("stops"))
        .withColumn("stops", stops_int[F.col("stops")].cast(IntegerType()))
        .withColumn("cabin_class", F.col("class"))
        .withColumn("duration_hours", F.col("duration").cast(DoubleType()))
        .withColumn("days_left_raw", F.col("days_left"))
        .withColumn("days_left", F.col("days_left").cast(IntegerType()))
        .withColumn("price_inr", F.col("price").cast(IntegerType()))
        .withColumn("route", F.concat_ws("-", F.col("source_city"), F.col("destination_city")))
        .withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in config.BUSINESS_COLUMNS],
                ),
                256,
            ),
        )
    )


# --------------------------------------------------------------------------- #
# 2. Validation rules
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Rule:
    name: str
    severity: str  # "error" | "warn"
    failed: Callable[[], Column]  # expression that is TRUE when the rule FAILS
    description: str


def _in(col: str, values: set[str]) -> Column:
    return F.col(col).isin(*sorted(values))


_RAW_ALIAS = {"stops": "stops_raw", "days_left": "days_left_raw"}


RULES: list[Rule] = [
    # -- structural -----------------------------------------------------------
    Rule("corrupt_record", "error", lambda: F.col("_corrupt_record").isNotNull(),
         "Row could not be parsed against the declared column list"),
    # -- completeness (raw copies used where standardize() overwrote the column) --
    *[
        Rule(f"missing_{c}", "error", (lambda c=c: F.col(_RAW_ALIAS.get(c, c)).isNull()), f"{c} is null/blank")
        for c in config.BUSINESS_COLUMNS
    ],
    # -- domain / referential -------------------------------------------------
    Rule("unknown_airline", "error", lambda: F.col("airline").isNotNull() & F.col("airline_code").isNull(),
         "airline not in reference list"),
    Rule("unknown_source_city", "error", lambda: F.col("source_city").isNotNull() & ~_in("source_city", config.VALID_CITIES),
         "source_city not in served-city list"),
    Rule("unknown_destination_city", "error",
         lambda: F.col("destination_city").isNotNull() & ~_in("destination_city", config.VALID_CITIES),
         "destination_city not in served-city list"),
    Rule("unknown_departure_time", "error",
         lambda: F.col("departure_time").isNotNull() & ~_in("departure_time", config.VALID_TIME_SLOTS),
         "departure_time not a known slot"),
    Rule("unknown_arrival_time", "error",
         lambda: F.col("arrival_time").isNotNull() & ~_in("arrival_time", config.VALID_TIME_SLOTS),
         "arrival_time not a known slot"),
    Rule("unknown_stops", "error", lambda: F.col("stops_raw").isNotNull() & F.col("stops").isNull(),
         "stops not in {zero, one, two_or_more}"),
    Rule("unknown_class", "error", lambda: F.col("class").isNotNull() & ~_in("class", config.VALID_CLASSES),
         "class not in {Economy, Business}"),
    # -- type / range ---------------------------------------------------------
    Rule("duration_not_numeric", "error", lambda: F.col("duration").isNotNull() & F.col("duration_hours").isNull(),
         "duration could not be cast to double"),
    Rule("duration_out_of_range", "error",
         lambda: ~F.col("duration_hours").between(config.MIN_DURATION_HOURS, config.MAX_DURATION_HOURS),
         f"duration outside [{config.MIN_DURATION_HOURS}, {config.MAX_DURATION_HOURS}] hours"),
    Rule("days_left_not_numeric", "error", lambda: F.col("days_left_raw").isNotNull() & F.col("days_left").isNull(),
         "days_left could not be cast to int"),
    Rule("days_left_out_of_range", "error",
         lambda: ~F.col("days_left").between(config.MIN_DAYS_LEFT, config.MAX_DAYS_LEFT),
         f"days_left outside [{config.MIN_DAYS_LEFT}, {config.MAX_DAYS_LEFT}]"),
    Rule("price_not_numeric", "error", lambda: F.col("price").isNotNull() & F.col("price_inr").isNull(),
         "price could not be cast to int"),
    Rule("price_out_of_range", "error",
         lambda: ~F.col("price_inr").between(config.MIN_PRICE_INR, config.MAX_PRICE_INR),
         f"price outside [{config.MIN_PRICE_INR}, {config.MAX_PRICE_INR}] INR"),
    # -- business logic -------------------------------------------------------
    Rule("same_source_destination", "error", lambda: F.col("source_city") == F.col("destination_city"),
         "origin equals destination"),
    Rule("nonstop_duration_implausible", "error",
         lambda: (F.col("stops") == 0) & (F.col("duration_hours") > config.MAX_NONSTOP_DURATION_HOURS),
         f"non-stop flight longer than {config.MAX_NONSTOP_DURATION_HOURS}h"),
    # -- warnings (kept, flagged) --------------------------------------------
    Rule("flight_number_unrecoverable", "warn",
         lambda: F.col("flight_number_raw").isNotNull() & F.col("flight_number").isNull(),
         "flight number corrupted beyond repair (e.g. 0.00E+00); kept with NULL flight_number"),
    Rule("flight_number_repaired", "warn", lambda: F.col("flight_number_raw").rlike(_SCI_NOTATION),
         "flight number restored from Excel scientific notation"),
    Rule("flight_prefix_airline_mismatch", "warn",
         lambda: F.col("flight_number").isNotNull() & F.col("airline_code").isNotNull()
         & (F.col("flight_prefix") != F.col("airline_code")),
         "flight code prefix does not match the airline's IATA code"),
]

ERROR_RULES = [r for r in RULES if r.severity == "error"]
WARN_RULES = [r for r in RULES if r.severity == "warn"]


def _collect(rules: list[Rule]) -> Column:
    """Array of rule names whose failure expression evaluates TRUE (nulls treated as not-failed)."""
    flags = [F.when(F.coalesce(r.failed(), F.lit(False)), F.lit(r.name)) for r in rules]
    return F.array_compact(F.array(*flags))


def apply_rules(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("dq_errors", _collect(ERROR_RULES))
        .withColumn("dq_warnings", _collect(WARN_RULES))
        .withColumn("is_valid", F.size("dq_errors") == 0)
    )


# --------------------------------------------------------------------------- #
# 3. Split + 4. Deduplicate
# --------------------------------------------------------------------------- #
def split_valid_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    return df.filter(F.col("is_valid")), df.filter(~F.col("is_valid"))


def deduplicate(valid: DataFrame) -> DataFrame:
    """Exact-duplicate collapse within a snapshot.

    Two rows are duplicates only when *every* business column is identical
    (record_hash). Same itinerary with a different price is a distinct fare
    observation and is intentionally kept. The first-seen row (lowest bronze
    row id) survives; `duplicate_count` records how many copies arrived.
    """
    key = ["snapshot_date", "record_hash"]
    counts = valid.groupBy(*key).agg(F.count("*").alias("duplicate_count"), F.min("_bronze_row_id").alias("_keep_id"))
    return (
        valid.join(counts, key, "inner")
        .filter(F.col("_bronze_row_id") == F.col("_keep_id"))
        .drop("_keep_id")
    )


# --------------------------------------------------------------------------- #
# 5. Final Silver projection
# --------------------------------------------------------------------------- #
SILVER_COLUMNS = [
    "fare_record_key",
    "snapshot_date",
    "airline_code",
    "airline_name",
    "airline_raw",
    "flight_number",
    "flight_number_raw",
    "source_city",
    "destination_city",
    "route",
    "departure_time_slot",
    "departure_slot_order",
    "arrival_time_slot",
    "arrival_slot_order",
    "stops",
    "stops_raw",
    "cabin_class",
    "duration_hours",
    "days_left",
    "price_inr",
    "dq_warnings",
    "duplicate_count",
    "record_hash",
    "_batch_id",
    "_source_file",
    "_bronze_row_id",
    "_ingested_at",
    "_silver_processed_at",
]

# Rejected rows are written with their ORIGINAL raw values so the reason is inspectable.
def rejected_columns() -> list[Column]:
    # Built lazily: F.col() needs an active SparkContext, so this cannot run at import time.
    return [
        F.col("snapshot_date"),
        F.col("rejection_reasons"),
        F.col("dq_warnings"),
        *[F.col(_RAW_ALIAS.get(c, c)).alias(c) for c in config.EXPECTED_COLUMNS],
        F.col("_corrupt_record"),
        F.col("record_hash"),
        F.col("_batch_id"),
        F.col("_source_file"),
        F.col("_bronze_row_id"),
        F.col("_ingested_at"),
        F.col("_silver_processed_at"),
    ]


def finalize_valid(deduped: DataFrame) -> DataFrame:
    return (
        deduped.withColumn("fare_record_key", F.sha2(F.concat_ws("||", F.col("record_hash"), F.col("snapshot_date").cast("string")), 256))
        .withColumn("dq_warnings", F.concat_ws(",", F.col("dq_warnings")))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .select(*SILVER_COLUMNS)
    )


def finalize_rejected(rejected: DataFrame) -> DataFrame:
    return (
        rejected.withColumn("rejection_reasons", F.concat_ws(",", F.col("dq_errors")))
        .withColumn("dq_warnings", F.concat_ws(",", F.col("dq_warnings")))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .select(*rejected_columns())
    )


def rule_metrics(scored: DataFrame) -> DataFrame:
    """One row per (snapshot_date, batch, rule) with the number of rows that tripped it."""
    all_rules = F.array(*[F.lit(r.name) for r in RULES])
    hits = scored.select(
        "snapshot_date",
        "_batch_id",
        F.explode(F.concat(F.col("dq_errors"), F.col("dq_warnings"))).alias("rule_name"),
    ).groupBy("snapshot_date", "_batch_id", "rule_name").agg(F.count("*").alias("failed_rows"))

    scaffold = (
        scored.select("snapshot_date", "_batch_id").distinct()
        .withColumn("rule_name", F.explode(all_rules))
    )
    severity = F.create_map(*[x for r in RULES for x in (F.lit(r.name), F.lit(r.severity))])
    description = F.create_map(*[x for r in RULES for x in (F.lit(r.name), F.lit(r.description))])
    totals = scored.groupBy("snapshot_date", "_batch_id").agg(F.count("*").alias("evaluated_rows"))

    return (
        scaffold.join(hits, ["snapshot_date", "_batch_id", "rule_name"], "left")
        .join(totals, ["snapshot_date", "_batch_id"], "left")
        .withColumn("failed_rows", F.coalesce(F.col("failed_rows"), F.lit(0)))
        .withColumn("severity", severity[F.col("rule_name")])
        .withColumn("description", description[F.col("rule_name")])
        .withColumn("failure_rate", F.round(F.col("failed_rows") / F.col("evaluated_rows"), 6))
        .withColumn("evaluated_at", F.current_timestamp())
    )