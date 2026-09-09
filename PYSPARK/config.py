"""Central configuration for the PySpark layers (Bronze + Silver).

Everything that is a *decision* (paths, expected schema, business domains,
validation thresholds) lives here so it can be reviewed in one place.
"""
from __future__ import annotations

from pathlib import Path

# --------------------------------------------------------------------------- #
# Paths (relative to project root)
# --------------------------------------------------------------------------- #
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
LANDING_DIR = DATA_DIR / "landing"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
REGISTRY_DIR = DATA_DIR / "registry"

RAW_FILE = RAW_DIR / "airlines_flights_data.csv"
REGISTRY_FILE = REGISTRY_DIR / "ingestion_registry.jsonl"

BRONZE_TABLE = BRONZE_DIR / "flight_fares"
SILVER_TABLE = SILVER_DIR / "flight_fares"
SILVER_REJECTED_TABLE = SILVER_DIR / "flight_fares_rejected"
SILVER_DQ_RESULTS = SILVER_DIR / "dq_results"
SILVER_RECONCILIATION = SILVER_DIR / "reconciliation"

# --------------------------------------------------------------------------- #
# Source contract
# --------------------------------------------------------------------------- #
# Exact header expected from the supplier. Any deviation is a schema-drift event.
EXPECTED_COLUMNS: list[str] = [
    "index",
    "airline",
    "flight",
    "source_city",
    "departure_time",
    "stops",
    "arrival_time",
    "destination_city",
    "class",
    "duration",
    "days_left",
    "price",
]

# Business columns = everything except the supplier's positional surrogate.
BUSINESS_COLUMNS: list[str] = [c for c in EXPECTED_COLUMNS if c != "index"]

# File naming convention for daily deliveries: airlines_flights_YYYYMMDD.csv
SNAPSHOT_DATE_REGEX = r"(\d{8})"
SNAPSHOT_DATE_FORMAT = "%Y%m%d"

# --------------------------------------------------------------------------- #
# Reference / domain values (derived from profiling + airline domain knowledge)
# --------------------------------------------------------------------------- #
AIRLINE_IATA_PREFIX: dict[str, str] = {
    "SpiceJet": "SG",
    "AirAsia": "I5",
    "Vistara": "UK",
    "Air_India": "AI",
    "Indigo": "6E",
    "GO_FIRST": "G8",
}

AIRLINE_DISPLAY_NAME: dict[str, str] = {
    "SpiceJet": "SpiceJet",
    "AirAsia": "AirAsia India",
    "Vistara": "Vistara",
    "Air_India": "Air India",
    "Indigo": "IndiGo",
    "GO_FIRST": "Go First",
}

VALID_CITIES = {"Delhi", "Mumbai", "Bangalore", "Kolkata", "Hyderabad", "Chennai"}
VALID_TIME_SLOTS = {"Early_Morning", "Morning", "Afternoon", "Evening", "Night", "Late_Night"}
VALID_STOPS = {"zero", "one", "two_or_more"}
VALID_CLASSES = {"Economy", "Business"}

# Ordinal used for time-of-day so marts can sort chronologically.
TIME_SLOT_ORDER: dict[str, int] = {
    "Early_Morning": 1,
    "Morning": 2,
    "Afternoon": 3,
    "Evening": 4,
    "Night": 5,
    "Late_Night": 6,
}

STOPS_TO_INT: dict[str, int] = {"zero": 0, "one": 1, "two_or_more": 2}

# --------------------------------------------------------------------------- #
# Validation thresholds
# --------------------------------------------------------------------------- #
MIN_PRICE_INR = 1
MAX_PRICE_INR = 500_000            # generous ceiling; anything above is a data error
MIN_DURATION_HOURS = 0.5           # shortest Indian domestic sector ~45 min
MAX_DURATION_HOURS = 60.0
MAX_NONSTOP_DURATION_HOURS = 6.0   # longest nonstop Indian domestic ~4h; 6h leaves headroom
MIN_DAYS_LEFT = 0
MAX_DAYS_LEFT = 365

CURRENCY = "INR"