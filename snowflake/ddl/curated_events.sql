-- ============================================================================
-- curated_events.sql
-- Typed, cleaned events table. The Stream + Task in streams_tasks.sql merges
-- new rows from RAW.RAW_EVENTS into here every minute.
-- ============================================================================

USE ROLE TRANSFORM_ROLE;
USE DATABASE STREAMING_DB;
USE SCHEMA CURATED;

CREATE TABLE IF NOT EXISTS EVENTS (
    EVENT_ID         STRING        NOT NULL,
    EVENT_TYPE       STRING        NOT NULL,
    EVENT_TIME       TIMESTAMP_NTZ NOT NULL,
    EVENT_DATE       DATE          NOT NULL,
    USER_ID          STRING,
    SESSION_ID       STRING,
    SOURCE           STRING,
    -- order-specific columns (nullable for non-order events)
    ORDER_ID         STRING,
    PRODUCT_ID       STRING,
    PRODUCT_NAME     STRING,
    CATEGORY         STRING,
    QUANTITY         NUMBER(10,0),
    UNIT_PRICE       NUMBER(12,2),
    AMOUNT           NUMBER(14,2),
    CURRENCY         STRING,
    IS_HIGH_VALUE    BOOLEAN,
    PAYMENT_METHOD   STRING,
    -- page_view columns
    PAGE             STRING,
    REFERRER         STRING,
    USER_AGENT       STRING,
    -- signup columns
    EMAIL            STRING,
    COUNTRY          STRING,
    REFERRAL_SOURCE  STRING,
    -- enrichment from the Lambda
    USER_SEGMENT     STRING,
    USER_COUNTRY     STRING,
    USER_LTV         NUMBER(14,2),
    -- bookkeeping
    SOURCE_FILE      STRING,
    SOURCE_ROW       NUMBER,
    INGESTED_AT      TIMESTAMP_NTZ,
    CURATED_AT       TIMESTAMP_NTZ DEFAULT SYSDATE(),
    CONSTRAINT PK_EVENTS PRIMARY KEY (EVENT_ID)
)
CLUSTER BY (EVENT_DATE, EVENT_TYPE)
COMMENT = 'Typed and de-duplicated event store, refreshed every minute by TASK MERGE_RAW_TO_CURATED.';
