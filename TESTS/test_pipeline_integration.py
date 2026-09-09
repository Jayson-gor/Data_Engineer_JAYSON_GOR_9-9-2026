"""Integration checks against the real data/ lake after `run_pipeline.sh full`.

Skipped automatically when the lake has not been built yet. Uses DuckDB to read
the Parquet outputs so these checks are independent of the Spark code under test.
"""
import json

import duckdb
import pytest

import config
from prepare_landing import INJECTED_FAULTS

pytestmark = pytest.mark.skipif(
    not (config.SILVER_RECONCILIATION.exists() and config.REGISTRY_FILE.exists()),
    reason="lake not built - run `scripts/run_pipeline.sh full` first",
)


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    for name, path in {
        "bronze": config.BRONZE_TABLE,
        "silver": config.SILVER_TABLE,
        "rejected": config.SILVER_REJECTED_TABLE,
        "recon": config.SILVER_RECONCILIATION,
        "dq": config.SILVER_DQ_RESULTS,
    }.items():
        c.execute(
            f"create view {name} as select * from read_parquet('{path.as_posix()}/*/*.parquet', hive_partitioning = true)"
        )
    yield c
    c.close()


def _registry():
    return [json.loads(l) for l in config.REGISTRY_FILE.read_text().splitlines() if l.strip()]


def test_every_batch_reconciles(con):
    bad = con.execute("select batch_id from recon where not is_balanced").fetchall()
    assert bad == [], f"unbalanced batches: {bad}"


def test_bronze_equals_silver_plus_rejected_plus_duplicates(con):
    b, s, r, d = con.execute(
        """
        select (select count(*) from bronze),
               (select count(*) from silver),
               (select count(*) from rejected),
               (select coalesce(sum(duplicate_count - 1), 0) from silver)
        """
    ).fetchone()
    assert b == s + r + d


def test_duplicate_file_delivery_was_skipped():
    statuses = [e["status"] for e in _registry()]
    assert "skipped_duplicate_file" in statuses
    redelivery = [e for e in _registry() if e["file_name"].endswith("_redelivery.csv")]
    assert redelivery and all(e["status"] == "skipped_duplicate_file" for e in redelivery)


def test_injected_faults_are_rejected_with_reasons(con):
    reasons = dict(
        con.execute(
            """
            select reason, count(*) from (
                select unnest(string_split(rejection_reasons, ',')) as reason
                from rejected where snapshot_date = date '2026-09-02'
            ) group by 1
        """
        ).fetchall()
    )
    assert reasons.get("price_out_of_range", 0) >= INJECTED_FAULTS["negative_price"]
    assert reasons.get("unknown_destination_city", 0) >= INJECTED_FAULTS["unknown_city"]
    assert reasons.get("same_source_destination", 0) >= INJECTED_FAULTS["same_source_destination"]
    assert reasons.get("missing_airline", 0) >= INJECTED_FAULTS["missing_airline"]
    assert reasons.get("nonstop_duration_implausible", 0) >= INJECTED_FAULTS["nonstop_too_long"]


def test_injected_exact_duplicates_were_collapsed(con):
    removed = con.execute(
        "select sum(duplicate_count - 1) from silver where snapshot_date = date '2026-09-02'"
    ).fetchone()[0]
    assert removed == INJECTED_FAULTS["exact_duplicates"]


def test_excel_corrupted_flight_numbers_repaired(con):
    bad_left = con.execute(
        "select count(*) from silver where flight_number like '%.00E%' or flight_number like '%E+%'"
    ).fetchone()[0]
    assert bad_left == 0
    repaired = con.execute(
        "select count(*) from silver where flight_number_raw like '%.00E-%' and flight_number is not null"
    ).fetchone()[0]
    assert repaired > 4000  # 4,705 in the supplied file


def test_silver_has_no_invalid_business_values(con):
    assert con.execute("select count(*) from silver where price_inr <= 0").fetchone()[0] == 0
    assert con.execute("select count(*) from silver where source_city = destination_city").fetchone()[0] == 0
    assert con.execute("select count(*) from silver where stops = 0 and duration_hours > 6").fetchone()[0] == 0
    assert con.execute("select count(*) from silver where fare_record_key is null").fetchone()[0] == 0
    dup_keys = con.execute(
        "select count(*) from (select fare_record_key from silver group by 1 having count(*) > 1)"
    ).fetchone()[0]
    assert dup_keys == 0


def test_gold_matches_silver_if_warehouse_exists():
    wh = config.DATA_DIR / "warehouse.duckdb"
    if not wh.exists():
        pytest.skip("dbt has not run yet")
    c = duckdb.connect(str(wh), read_only=True)
    try:
        gold = c.execute("select snapshot_date, count(*) from marts.fct_fare_observations group by 1 order by 1").fetchall()
        silver = c.execute(
            f"select snapshot_date, count(*) from read_parquet('{config.SILVER_TABLE.as_posix()}/*/*.parquet', hive_partitioning = true) group by 1 order by 1"
        ).fetchall()
        assert gold == silver
    finally:
        c.close()