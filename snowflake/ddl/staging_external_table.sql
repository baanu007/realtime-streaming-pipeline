-- ============================================================================
-- staging_external_table.sql
-- External table over the same S3 prefix Firehose writes to. Useful for
-- backfills, ad-hoc analytics, and reconciliation against the RAW table.
-- ============================================================================

USE ROLE INGEST_ROLE;
USE DATABASE STREAMING_DB;
USE SCHEMA STAGING;

-- 1. Storage integration (one-time setup, account-level)
--    Replace placeholders before running, then comment this out.
-- CREATE STORAGE INTEGRATION IF NOT EXISTS S3_EVENTS_INT
--   TYPE = EXTERNAL_STAGE
--   STORAGE_PROVIDER = S3
--   ENABLED = TRUE
--   STORAGE_AWS_ROLE_ARN = '<SNOWFLAKE_S3_INTEGRATION_ROLE_ARN>'
--   STORAGE_ALLOWED_LOCATIONS = ('s3://<EVENTS_LANDING_BUCKET>/events/');

-- 2. File format for parquet output from Firehose
CREATE FILE FORMAT IF NOT EXISTS PARQUET_EVENTS_FMT
    TYPE = PARQUET
    COMPRESSION = SNAPPY;

-- 3. External stage
CREATE STAGE IF NOT EXISTS EVENTS_STAGE
    STORAGE_INTEGRATION = S3_EVENTS_INT
    URL = 's3://<EVENTS_LANDING_BUCKET>/events/'
    FILE_FORMAT = PARQUET_EVENTS_FMT;

-- 4. External table partitioned the same way Firehose writes
CREATE OR REPLACE EXTERNAL TABLE EVENTS_EXT (
    EVENT_TYPE STRING AS (SPLIT_PART(METADATA$FILENAME, '/', 2)),
    DT         DATE   AS (TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', 3), '=', 2), 'YYYY-MM-DD')),
    HH         NUMBER AS (TRY_TO_NUMBER(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', 4), '=', 2))),
    VALUE      VARIANT
)
PARTITION BY (EVENT_TYPE, DT, HH)
LOCATION = @EVENTS_STAGE
FILE_FORMAT = PARQUET_EVENTS_FMT
AUTO_REFRESH = TRUE
COMMENT = 'External view over the Firehose S3 landing zone.';
