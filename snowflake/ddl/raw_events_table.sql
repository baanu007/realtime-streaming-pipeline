-- ============================================================================
-- raw_events_table.sql
-- VARIANT landing table for Snowpipe. Every record from S3 lands here first
-- so we have a replayable, schema-on-read source of truth before applying
-- typed transformations downstream.
-- ============================================================================

USE ROLE INGEST_ROLE;
USE DATABASE STREAMING_DB;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS RAW_EVENTS (
    RAW_ID         NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    PAYLOAD        VARIANT       NOT NULL,
    EVENT_TYPE     STRING        AS (PAYLOAD:event_type::STRING),
    EVENT_TIME     TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(PAYLOAD:event_time::STRING)),
    USER_ID        STRING        AS (PAYLOAD:user_id::STRING),
    SOURCE_FILE    STRING,
    SOURCE_ROW     NUMBER,
    INGESTED_AT    TIMESTAMP_NTZ DEFAULT SYSDATE(),
    PIPELINE       STRING        DEFAULT 'snowpipe-firehose'
)
COMMENT = 'Raw landing zone for events arriving via Snowpipe from S3.';

-- Lightweight clustering: most ad-hoc queries filter by event_type and day.
ALTER TABLE RAW_EVENTS CLUSTER BY (EVENT_TYPE, TO_DATE(EVENT_TIME));
