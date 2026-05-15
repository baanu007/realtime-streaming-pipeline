-- ============================================================================
-- streams_tasks.sql
-- Stream on RAW.RAW_EVENTS + a 1-minute Task that merges new rows into
-- CURATED.EVENTS with idempotent semantics (MERGE on EVENT_ID).
-- ============================================================================

USE ROLE TRANSFORM_ROLE;
USE DATABASE STREAMING_DB;

-- ----------------------------------------------------------------------------
-- 1. Stream over the landing table
-- ----------------------------------------------------------------------------
USE SCHEMA RAW;

CREATE STREAM IF NOT EXISTS RAW_EVENTS_STREAM
    ON TABLE RAW_EVENTS
    APPEND_ONLY = TRUE
    COMMENT = 'Tracks new RAW_EVENTS rows for the curated merge task.';

-- ----------------------------------------------------------------------------
-- 2. Merge task running every minute
-- ----------------------------------------------------------------------------
USE SCHEMA CURATED;

CREATE OR REPLACE TASK MERGE_RAW_TO_CURATED
    WAREHOUSE = TRANSFORM_WH
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Idempotent merge from RAW.RAW_EVENTS into CURATED.EVENTS.'
    WHEN SYSTEM$STREAM_HAS_DATA('STREAMING_DB.RAW.RAW_EVENTS_STREAM')
AS
MERGE INTO CURATED.EVENTS tgt
USING (
    SELECT
        PAYLOAD:event_id::STRING                                AS EVENT_ID,
        PAYLOAD:event_type::STRING                              AS EVENT_TYPE,
        TRY_TO_TIMESTAMP_NTZ(PAYLOAD:event_time::STRING)        AS EVENT_TIME,
        TO_DATE(TRY_TO_TIMESTAMP_NTZ(PAYLOAD:event_time::STRING)) AS EVENT_DATE,
        PAYLOAD:user_id::STRING                                 AS USER_ID,
        PAYLOAD:session_id::STRING                              AS SESSION_ID,
        PAYLOAD:source::STRING                                  AS SOURCE,
        PAYLOAD:order_id::STRING                                AS ORDER_ID,
        PAYLOAD:product_id::STRING                              AS PRODUCT_ID,
        PAYLOAD:product_name::STRING                            AS PRODUCT_NAME,
        PAYLOAD:category::STRING                                AS CATEGORY,
        TRY_TO_NUMBER(PAYLOAD:quantity::STRING)                 AS QUANTITY,
        TRY_TO_DECIMAL(PAYLOAD:unit_price::STRING, 12, 2)       AS UNIT_PRICE,
        TRY_TO_DECIMAL(PAYLOAD:amount::STRING, 14, 2)           AS AMOUNT,
        PAYLOAD:currency::STRING                                AS CURRENCY,
        PAYLOAD:is_high_value::BOOLEAN                          AS IS_HIGH_VALUE,
        PAYLOAD:payment_method::STRING                          AS PAYMENT_METHOD,
        PAYLOAD:page::STRING                                    AS PAGE,
        PAYLOAD:referrer::STRING                                AS REFERRER,
        PAYLOAD:user_agent::STRING                              AS USER_AGENT,
        PAYLOAD:email::STRING                                   AS EMAIL,
        PAYLOAD:country::STRING                                 AS COUNTRY,
        PAYLOAD:referral_source::STRING                         AS REFERRAL_SOURCE,
        PAYLOAD:user_segment::STRING                            AS USER_SEGMENT,
        PAYLOAD:user_country::STRING                            AS USER_COUNTRY,
        TRY_TO_DECIMAL(PAYLOAD:user_lifetime_value::STRING, 14, 2) AS USER_LTV,
        SOURCE_FILE,
        SOURCE_ROW,
        INGESTED_AT
    FROM STREAMING_DB.RAW.RAW_EVENTS_STREAM
    WHERE PAYLOAD:event_id IS NOT NULL
      AND TRY_TO_TIMESTAMP_NTZ(PAYLOAD:event_time::STRING) IS NOT NULL
) src
ON tgt.EVENT_ID = src.EVENT_ID
WHEN NOT MATCHED THEN INSERT (
    EVENT_ID, EVENT_TYPE, EVENT_TIME, EVENT_DATE, USER_ID, SESSION_ID, SOURCE,
    ORDER_ID, PRODUCT_ID, PRODUCT_NAME, CATEGORY, QUANTITY, UNIT_PRICE, AMOUNT,
    CURRENCY, IS_HIGH_VALUE, PAYMENT_METHOD, PAGE, REFERRER, USER_AGENT,
    EMAIL, COUNTRY, REFERRAL_SOURCE, USER_SEGMENT, USER_COUNTRY, USER_LTV,
    SOURCE_FILE, SOURCE_ROW, INGESTED_AT
) VALUES (
    src.EVENT_ID, src.EVENT_TYPE, src.EVENT_TIME, src.EVENT_DATE, src.USER_ID, src.SESSION_ID, src.SOURCE,
    src.ORDER_ID, src.PRODUCT_ID, src.PRODUCT_NAME, src.CATEGORY, src.QUANTITY, src.UNIT_PRICE, src.AMOUNT,
    src.CURRENCY, src.IS_HIGH_VALUE, src.PAYMENT_METHOD, src.PAGE, src.REFERRER, src.USER_AGENT,
    src.EMAIL, src.COUNTRY, src.REFERRAL_SOURCE, src.USER_SEGMENT, src.USER_COUNTRY, src.USER_LTV,
    src.SOURCE_FILE, src.SOURCE_ROW, src.INGESTED_AT
);

-- Tasks are created suspended; resume after wiring up grants and warehouse.
ALTER TASK MERGE_RAW_TO_CURATED RESUME;
