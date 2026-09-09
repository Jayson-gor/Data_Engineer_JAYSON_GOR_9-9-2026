# Architecture

## End-to-end flow

```mermaid
flowchart LR
    subgraph SRC["Source"]
        CSV["Daily CSV<br/>airlines_flights_YYYYMMDD.csv"]
    end

    subgraph BRONZE["BRONZE  (PySpark → Parquet)"]
        B1["Header contract check<br/>(schema drift → reject)"]
        B2["SHA-256 file registry<br/>(duplicate file → skip)"]
        B3["All columns STRING<br/>+ _corrupt_record<br/>+ ingestion metadata"]
        B4[("bronze/flight_fares<br/>partition: snapshot_date")]
        B1 --> B2 --> B3 --> B4
    end

    subgraph SILVER["SILVER  (PySpark → Parquet)"]
        S1["standardize()<br/>types · trim · IATA codes<br/>flight-no. repair 6.00E-269→6E-269"]
        S2["apply_rules()<br/>26 rules · error / warn"]
        S3["deduplicate()<br/>exact dupes on record_hash"]
        S4[("silver/flight_fares")]
        S5[("silver/flight_fares_rejected<br/>+ rejection_reasons")]
        S6[("silver/dq_results<br/>silver/reconciliation")]
        S1 --> S2 --> S3 --> S4
        S2 --> S5
        S2 --> S6
    end

    subgraph GOLD["GOLD  (dbt → DuckDB local / Redshift prod)"]
        G1["staging views<br/>stg_silver__*"]
        G2["intermediate (ephemeral)<br/>int_fare_observations_enriched"]
        G3["core star schema<br/>fct_fare_observations<br/>dim_airline · dim_route · dim_time_slot<br/>dim_cabin_class · dim_date"]
        G4["analytics marts (incremental)<br/>mart_route_pricing<br/>mart_booking_lead_time<br/>mart_airline_performance<br/>mart_time_slot_demand<br/>mart_data_quality"]
        G1 --> G2 --> G3 --> G4
    end

    subgraph RS["AMAZON REDSHIFT"]
        R1["COPY from S3 Parquet<br/>→ DELETE/INSERT by snapshot_date"]
        R2[("silver.* · marts.* · ops.*<br/>DISTKEY route · SORTKEY snapshot_date")]
        R3["Spectrum → Bronze on S3"]
        R1 --> R2
    end

    CSV --> B1
    B4 --> S1
    S4 --> G1
    S5 -. audit .-> G1
    S6 -. observability .-> G1
    S4 -- "S3 sync" --> R1
    B4 -. "S3" .-> R3

    subgraph CTRL["Control plane"]
        REG["data/registry/ingestion_registry.jsonl<br/>file_hash · batch_id · status · counts"]
        TESTS["pytest (unit + integration)<br/>dbt tests (48)"]
        MON["Monitoring: rejection rate · freshness<br/>reconciliation · schema drift"]
    end
    B2 <--> REG
    S3 --> REG