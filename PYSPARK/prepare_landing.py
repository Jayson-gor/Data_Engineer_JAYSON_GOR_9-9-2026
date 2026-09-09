"""Simulate the supplier's landing zone.

The assignment supplies ONE undated CSV but asks for a daily-file design.
This script (pandas; it is test tooling, not part of the pipeline) creates:

  data/landing/airlines_flights_20260901.csv        full initial load (verbatim copy)
  data/landing/airlines_flights_20260902.csv        daily delta with deliberate DQ faults
  data/landing/airlines_flights_20260902_redelivery.csv  byte-identical re-delivery (duplicate file)
  data/landing/airlines_flights_20260903.csv        clean daily delta
  data/landing/late/airlines_flights_20260831.csv   late-arriving file (NOT auto-ingested)

Faults injected in 20260902 are documented so the Silver rejection counts can be
asserted in tests/.
"""
from __future__ import annotations

import argparse
import shutil

import numpy as np
import pandas as pd

import config
from common import get_logger

log = get_logger("prepare_landing")

SEED = 42
DELTA_FRACTION = 0.02

# Exact number of faulty rows injected into the 20260902 file (used by tests).
INJECTED_FAULTS = {
    "exact_duplicates": 50,
    "negative_price": 20,
    "unknown_city": 20,
    "same_source_destination": 10,
    "missing_airline": 10,
    "nonstop_too_long": 10,
}


def _daily_delta(base: pd.DataFrame, rng: np.random.Generator, frac: float) -> pd.DataFrame:
    """A plausible next-day observation: same itineraries, one day closer, some prices moved."""
    delta = base.sample(frac=frac, random_state=int(rng.integers(0, 1 << 31))).copy()
    delta["days_left"] = (delta["days_left"] - 1).clip(lower=1)
    moved = rng.random(len(delta)) < 0.4
    factor = rng.uniform(0.90, 1.15, size=len(delta))
    delta.loc[moved, "price"] = (delta.loc[moved, "price"] * factor[moved]).round().astype(int)
    return delta


def _inject_faults(delta: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    n = INJECTED_FAULTS
    pick = lambda k: delta.sample(n=n[k], random_state=int(rng.integers(0, 1 << 31))).copy()

    dup = pick("exact_duplicates")

    neg = pick("negative_price")
    neg["price"] = -neg["price"]

    city = pick("unknown_city")
    city["destination_city"] = "Goa"

    same = pick("same_source_destination")
    same["destination_city"] = same["source_city"]

    miss = pick("missing_airline")
    miss["airline"] = ""

    long_ns = pick("nonstop_too_long")
    long_ns["stops"] = "zero"
    long_ns["duration"] = 14.5

    out = pd.concat([delta, dup, neg, city, same, miss, long_ns], ignore_index=True)
    return out.sample(frac=1.0, random_state=SEED).reset_index(drop=True)


def _write(df: pd.DataFrame, name: str, subdir: str | None = None) -> None:
    target_dir = config.LANDING_DIR / subdir if subdir else config.LANDING_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    df["index"] = range(len(df))
    df = df[config.EXPECTED_COLUMNS]
    path = target_dir / name
    df.to_csv(path, index=False)
    log.info("wrote %-45s rows=%d", path.relative_to(config.PROJECT_ROOT), len(df))


def main(force: bool = False) -> None:
    initial = config.LANDING_DIR / "airlines_flights_20260901.csv"
    if initial.exists() and not force:
        log.info("landing zone already prepared (use --force to regenerate)")
        return

    if not config.RAW_FILE.exists():
        raise FileNotFoundError(f"raw source not found: {config.RAW_FILE}")

    rng = np.random.default_rng(SEED)
    base = pd.read_csv(config.RAW_FILE, dtype=str)
    for c in ("duration",):
        base[c] = base[c].astype(float)
    for c in ("days_left", "price"):
        base[c] = base[c].astype(int)

    config.LANDING_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(config.RAW_FILE, initial)
    log.info("wrote %-45s (verbatim copy of raw)", initial.relative_to(config.PROJECT_ROOT))

    day2 = _inject_faults(_daily_delta(base, rng, DELTA_FRACTION), rng)
    _write(day2, "airlines_flights_20260902.csv")
    shutil.copyfile(
        config.LANDING_DIR / "airlines_flights_20260902.csv",
        config.LANDING_DIR / "airlines_flights_20260902_redelivery.csv",
    )
    log.info("wrote airlines_flights_20260902_redelivery.csv (byte-identical duplicate delivery)")

    _write(_daily_delta(base, rng, DELTA_FRACTION), "airlines_flights_20260903.csv")
    _write(_daily_delta(base, rng, DELTA_FRACTION / 2), "airlines_flights_20260831.csv", subdir="late")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="regenerate even if files exist")
    main(force=ap.parse_args().force)