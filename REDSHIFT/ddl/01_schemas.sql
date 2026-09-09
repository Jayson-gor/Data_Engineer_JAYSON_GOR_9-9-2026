-- Medallion layers map to schemas. Bronze is NOT copied into Redshift: it stays
-- in S3 (Parquet) and is exposed through Redshift Spectrum for audit queries.
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS staging;   -- dbt staging views
CREATE SCHEMA IF NOT EXISTS marts;     -- dbt dimensions, facts, analytical marts
CREATE SCHEMA IF NOT EXISTS ops;       -- load control / audit tables

-- Bronze via Spectrum (requires an IAM role with S3 read + Glue catalog access)
-- CREATE EXTERNAL SCHEMA bronze
--   FROM DATA CATALOG DATABASE 'airline_bronze'
--   IAM_ROLE 'arn:aws:iam::<account>:role/redshift-spectrum'
--   CREATE EXTERNAL DATABASE IF NOT EXISTS;