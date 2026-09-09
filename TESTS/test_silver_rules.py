"""Unit tests for the pure Silver transformations."""
from pyspark.sql import functions as F

import silver_rules as rules
from conftest import GOOD_ROWS, HEADER


def _bronze_df(spark, rows):
    """Build a Bronze-shaped DataFrame (all strings + metadata) from CSV lines."""
    cols = HEADER.split(",")
    data = [dict(zip(cols, r.split(","))) for r in rows]
    for i, d in enumerate(data):
        d.update(
            _corrupt_record=None,
            _bronze_row_id=i,
            _batch_id="b1",
            _source_file="f.csv",
            _ingested_at="2026-09-01T00:00:00",
            snapshot_date="2026-09-01",
        )
    df = spark.createDataFrame(data)
    return df.withColumn("snapshot_date", F.col("snapshot_date").cast("date")).withColumn(
        "_ingested_at", F.col("_ingested_at").cast("timestamp")
    )


def _scored(spark, rows):
    return rules.apply_rules(rules.standardize(_bronze_df(spark, rows))).collect()


# --------------------------------------------------------------------------- #
# flight-number repair
# --------------------------------------------------------------------------- #
def test_scientific_notation_flight_is_repaired(spark):
    row = _scored(spark, [GOOD_ROWS[2]])[0]
    assert row.flight_number == "6E-269"
    assert row.flight_number_raw == "6.00E-269"
    assert "flight_number_repaired" in row.dq_warnings
    assert row.is_valid


def test_unrecoverable_flight_is_kept_with_warning(spark):
    row = _scored(spark, [GOOD_ROWS[3]])[0]
    assert row.flight_number is None
    assert "flight_number_unrecoverable" in row.dq_warnings
    assert row.is_valid, "unrecoverable flight number is a WARN, not a rejection"


def test_clean_flight_untouched(spark):
    row = _scored(spark, [GOOD_ROWS[0]])[0]
    assert row.flight_number == "SG-8709"
    assert row.dq_warnings == []


# --------------------------------------------------------------------------- #
# standardization
# --------------------------------------------------------------------------- #
def test_types_and_mappings(spark):
    row = _scored(spark, [GOOD_ROWS[0]])[0]
    assert row.airline_code == "SG"
    assert row.stops == 0
    assert row.departure_slot_order == 4
    assert isinstance(row.duration_hours, float) and row.duration_hours == 2.17
    assert row.price_inr == 5953 and row.days_left == 1
    assert row.route == "Delhi-Mumbai"


# --------------------------------------------------------------------------- #
# error rules -> rejection reasons
# --------------------------------------------------------------------------- #
def _bad(base: str, **overrides) -> str:
    cols = HEADER.split(",")
    vals = dict(zip(cols, base.split(",")))
    vals.update(overrides)
    return ",".join(vals[c] for c in cols)


def test_negative_price_rejected(spark):
    row = _scored(spark, [_bad(GOOD_ROWS[0], price="-100")])[0]
    assert not row.is_valid and "price_out_of_range" in row.dq_errors


def test_unknown_city_rejected(spark):
    row = _scored(spark, [_bad(GOOD_ROWS[0], destination_city="Goa")])[0]
    assert "unknown_destination_city" in row.dq_errors


def test_same_origin_destination_rejected(spark):
    row = _scored(spark, [_bad(GOOD_ROWS[0], destination_city="Delhi")])[0]
    assert "same_source_destination" in row.dq_errors


def test_blank_airline_rejected(spark):
    row = _scored(spark, [_bad(GOOD_ROWS[0], airline="")])[0]
    assert "missing_airline" in row.dq_errors
    assert "unknown_airline" not in row.dq_errors, "blank should be reported as missing, not unknown"


def test_nonstop_too_long_rejected(spark):
    row = _scored(spark, [_bad(GOOD_ROWS[0], stops="zero", duration="14.5")])[0]
    assert "nonstop_duration_implausible" in row.dq_errors


def test_non_numeric_price_rejected(spark):
    row = _scored(spark, [_bad(GOOD_ROWS[0], price="abc")])[0]
    assert "price_not_numeric" in row.dq_errors


def test_multiple_reasons_are_all_captured(spark):
    row = _scored(spark, [_bad(GOOD_ROWS[0], price="-1", destination_city="Delhi")])[0]
    assert {"price_out_of_range", "same_source_destination"} <= set(row.dq_errors)


# --------------------------------------------------------------------------- #
# de-duplication
# --------------------------------------------------------------------------- #
def test_exact_duplicates_collapse_but_price_variants_survive(spark):
    rows = [
        GOOD_ROWS[0],
        _bad(GOOD_ROWS[0], index="9"),           # identical business columns -> duplicate
        _bad(GOOD_ROWS[0], index="10", price="6100"),  # same itinerary, new price -> distinct fare
    ]
    df = rules.apply_rules(rules.standardize(_bronze_df(spark, rows)))
    valid, _ = rules.split_valid_rejected(df)
    out = rules.deduplicate(valid).orderBy("_bronze_row_id").collect()
    assert len(out) == 2
    assert out[0].duplicate_count == 2 and out[0].price_inr == 5953
    assert out[1].duplicate_count == 1 and out[1].price_inr == 6100


def test_rule_metrics_cover_every_rule(spark):
    df = rules.apply_rules(rules.standardize(_bronze_df(spark, GOOD_ROWS)))
    metrics = rules.rule_metrics(df).collect()
    assert {m.rule_name for m in metrics} == {r.name for r in rules.RULES}
    repaired = next(m for m in metrics if m.rule_name == "flight_number_repaired")
    assert repaired.failed_rows == 1 and repaired.evaluated_rows == 4