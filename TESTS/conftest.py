import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# Add the scripts folder itself (NOT the project root, which would shadow the pyspark library).
sys.path.insert(0, str(PROJECT_ROOT / "pyspark"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def isolated_lake(tmp_path, monkeypatch):
    """Point every config path at a temp dir so tests never touch data/."""
    import config

    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "LANDING_DIR", tmp_path / "landing")
    monkeypatch.setattr(config, "BRONZE_DIR", tmp_path / "bronze")
    monkeypatch.setattr(config, "SILVER_DIR", tmp_path / "silver")
    monkeypatch.setattr(config, "REGISTRY_DIR", tmp_path / "registry")
    monkeypatch.setattr(config, "REGISTRY_FILE", tmp_path / "registry" / "ingestion_registry.jsonl")
    monkeypatch.setattr(config, "BRONZE_TABLE", tmp_path / "bronze" / "flight_fares")
    monkeypatch.setattr(config, "SILVER_TABLE", tmp_path / "silver" / "flight_fares")
    monkeypatch.setattr(config, "SILVER_REJECTED_TABLE", tmp_path / "silver" / "flight_fares_rejected")
    monkeypatch.setattr(config, "SILVER_DQ_RESULTS", tmp_path / "silver" / "dq_results")
    monkeypatch.setattr(config, "SILVER_RECONCILIATION", tmp_path / "silver" / "reconciliation")
    (tmp_path / "landing").mkdir()
    return tmp_path


HEADER = "index,airline,flight,source_city,departure_time,stops,arrival_time,destination_city,class,duration,days_left,price"

GOOD_ROWS = [
    "0,SpiceJet,SG-8709,Delhi,Evening,zero,Night,Mumbai,Economy,2.17,1,5953",
    "1,Vistara,UK-995,Delhi,Morning,zero,Afternoon,Mumbai,Economy,2.25,1,5955",
    "2,Indigo,6.00E-269,Mumbai,Night,one,Morning,Kolkata,Business,12.5,10,45000",
    "3,Indigo,0.00E+00,Mumbai,Night,one,Morning,Kolkata,Economy,12.5,10,4500",
]


def write_csv(path: Path, rows, header: str = HEADER) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")
    return path