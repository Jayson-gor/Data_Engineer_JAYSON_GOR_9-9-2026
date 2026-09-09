#!/usr/bin/env bash
# Single entrypoint for the pipeline. Runs from the project root.
#
# Usage:
#   run_pipeline.sh full                  # prepare landing files -> bronze -> silver -> dbt -> tests
#   run_pipeline.sh daily <path/to.csv>   # incremental: ingest one new daily file -> silver -> dbt
#   run_pipeline.sh bronze [file|dir]     # bronze only (defaults to data/landing)
#   run_pipeline.sh silver                # silver only
#   run_pipeline.sh dbt                   # dbt build only
#   run_pipeline.sh test                  # pytest + dbt test
#   run_pipeline.sh reset                 # wipe bronze/silver/registry/warehouse (keeps landing)
set -euo pipefail

cd "$(dirname "$0")/.."
# NOTE: project root is intentionally NOT on PYTHONPATH - the mandated ./pyspark
# folder would otherwise shadow the real pyspark library.
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$(pwd)/dbt}"

MODE="${1:-full}"
ARG="${2:-}"

log() { printf '\n\033[1;34m[%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*"; }

prepare_landing() {
  log "STEP 0  Preparing landing zone (initial file + simulated daily deliveries)"
  python pyspark/prepare_landing.py
}

run_bronze() {
  log "STEP 1  Bronze ingestion (PySpark) <- ${1:-data/landing}"
  python pyspark/bronze_ingest.py --input "${1:-data/landing}"
}

run_silver() {
  log "STEP 2  Silver transformation (PySpark)"
  python pyspark/silver_transform.py
}

run_dbt() {
  log "STEP 3  dbt build (staging -> intermediate -> marts) on DuckDB"
  (cd dbt && dbt build --profiles-dir . "$@")
}

run_tests() {
  log "STEP 4  pytest (unit + idempotency + reconciliation)"
  pytest -q tests
}

reset_all() {
  log "Resetting derived data"
  rm -rf data/bronze data/silver data/registry data/warehouse.duckdb data/warehouse.duckdb.wal dbt/target spark-warehouse
}

case "$MODE" in
  full)
    prepare_landing
    run_bronze
    run_silver
    run_dbt
    run_tests
    ;;
  daily)
    [[ -z "$ARG" ]] && { echo "daily mode requires a file path"; exit 2; }
    run_bronze "$ARG"
    run_silver
    run_dbt
    ;;
  bronze) run_bronze "$ARG" ;;
  silver) run_silver ;;
  dbt)    run_dbt ;;
  test)   run_tests; (cd dbt && dbt test --profiles-dir .) ;;
  reset)  reset_all ;;
  *) echo "Unknown mode: $MODE"; exit 2 ;;
esac

log "DONE ($MODE)"