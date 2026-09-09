"""Bronze ingestion behaviour: idempotency, duplicate files, schema drift, supersede."""
import bronze_ingest as bronze
import config
from common import load_registry
from conftest import GOOD_ROWS, HEADER, write_csv


def _bronze_rows(spark):
    return spark.read.parquet(str(config.BRONZE_TABLE))


def test_ingest_stamps_metadata_and_partitions(spark, isolated_lake):
    f = write_csv(config.LANDING_DIR / "airlines_flights_20260901.csv", GOOD_ROWS)
    entry = bronze.ingest_file(spark, f)

    assert entry["status"] == "bronze_done"
    assert entry["bronze_row_count"] == 4 and entry["source_to_bronze_reconciled"]

    df = _bronze_rows(spark)
    assert df.count() == 4
    assert {"_source_file", "_source_file_hash", "_ingested_at", "_batch_id", "snapshot_date"} <= set(df.columns)
    assert df.select("snapshot_date").distinct().collect()[0][0].isoformat() == "2026-09-01"
    # raw preservation: corrupted flight numbers arrive untouched
    assert df.filter(df.flight == "6.00E-269").count() == 1
    assert (config.BRONZE_TABLE / "snapshot_date=2026-09-01").exists()


def test_rerun_same_file_is_skipped_and_does_not_double_count(spark, isolated_lake):
    f = write_csv(config.LANDING_DIR / "airlines_flights_20260901.csv", GOOD_ROWS)
    bronze.ingest_file(spark, f)
    second = bronze.ingest_file(spark, f)

    assert second["status"] == "skipped_duplicate_file"
    assert _bronze_rows(spark).count() == 4


def test_byte_identical_redelivery_under_new_name_is_skipped(spark, isolated_lake):
    f1 = write_csv(config.LANDING_DIR / "airlines_flights_20260901.csv", GOOD_ROWS)
    f2 = write_csv(config.LANDING_DIR / "airlines_flights_20260901_redelivery.csv", GOOD_ROWS)
    bronze.ingest_file(spark, f1)
    assert bronze.ingest_file(spark, f2)["status"] == "skipped_duplicate_file"
    assert _bronze_rows(spark).count() == 4


def test_corrected_file_for_same_snapshot_supersedes_old_partition(spark, isolated_lake):
    f1 = write_csv(config.LANDING_DIR / "airlines_flights_20260901.csv", GOOD_ROWS)
    bronze.ingest_file(spark, f1)
    f2 = write_csv(config.LANDING_DIR / "airlines_flights_20260901_v2.csv", GOOD_ROWS[:2])
    entry = bronze.ingest_file(spark, f2)

    assert entry["status"] == "bronze_done"
    assert _bronze_rows(spark).count() == 2, "partition replaced, not appended"
    statuses = [e["status"] for e in load_registry()]
    assert "superseded" in statuses


def test_late_arriving_file_lands_in_its_own_partition(spark, isolated_lake):
    bronze.ingest_file(spark, write_csv(config.LANDING_DIR / "airlines_flights_20260903.csv", GOOD_ROWS))
    bronze.ingest_file(spark, write_csv(config.LANDING_DIR / "airlines_flights_20260831.csv", GOOD_ROWS[:1]))

    df = _bronze_rows(spark)
    dates = sorted(r[0].isoformat() for r in df.select("snapshot_date").distinct().collect())
    assert dates == ["2026-08-31", "2026-09-03"]
    assert df.count() == 5


def test_schema_drift_is_rejected_before_any_write(spark, isolated_lake):
    drifted = HEADER.replace("price", "fare_amount")
    f = write_csv(config.LANDING_DIR / "airlines_flights_20260901.csv", GOOD_ROWS, header=drifted)
    entry = bronze.ingest_file(spark, f)

    assert entry["status"] == "rejected_schema_drift"
    assert "missing=['price']" in entry["message"] and "extra=['fare_amount']" in entry["message"]
    assert not config.BRONZE_TABLE.exists()


def test_short_row_is_preserved_not_dropped(spark, isolated_lake):
    # Spark CSV (PERMISSIVE) pads short rows with NULLs; Bronze keeps the row and
    # Silver's missing_* rules reject it with an explicit reason.
    rows = GOOD_ROWS + ["99,SpiceJet,SG-1,Delhi,Evening"]
    f = write_csv(config.LANDING_DIR / "airlines_flights_20260901.csv", rows)
    entry = bronze.ingest_file(spark, f)

    assert entry["bronze_row_count"] == 5 and entry["source_to_bronze_reconciled"]
    df = _bronze_rows(spark)
    short = df.filter(df.index == "99").collect()[0]
    assert short.price is None and short.destination_city is None