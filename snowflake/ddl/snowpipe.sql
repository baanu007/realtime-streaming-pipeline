-- ============================================================================
-- snowpipe.sql
-- Snowpipe with S3 event notifications via SNS so files Firehose drops
-- in s3://<EVENTS_LANDING_BUCKET>/events/ are auto-ingested into RAW_EVENTS.
--
-- AWS side prerequisites (one-time):
--   1. SNS topic on the events landing bucket that fires on s3:ObjectCreated:*
--      for the events/ prefix.
--   2. Subscribe the SNS ARN that Snowflake returns from `DESCRIBE PIPE` to
--      that topic (SQS endpoint type "Amazon SQS"). Snowflake auto-creates
--      a hidden SQS queue per Snowpipe; we just need it subscribed.
-- ============================================================================

USE ROLE INGEST_ROLE;
USE DATABASE STREAMING_DB;
USE SCHEMA RAW;

CREATE OR REPLACE PIPE EVENTS_PIPE
    AUTO_INGEST = TRUE
    AWS_SNS_TOPIC = 'arn:aws:sns:<AWS_REGION>:<ACCOUNT_ID>:<S3_EVENT_NOTIFICATION_TOPIC>'
    COMMENT = 'Auto-ingests Firehose parquet files into RAW.RAW_EVENTS.'
    AS
COPY INTO RAW_EVENTS (PAYLOAD, SOURCE_FILE, SOURCE_ROW)
FROM (
    SELECT
        $1,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @STAGING.EVENTS_STAGE
)
FILE_FORMAT = (FORMAT_NAME = STAGING.PARQUET_EVENTS_FMT)
ON_ERROR = 'SKIP_FILE_10%'        -- tolerate up to 10% poison-pill rows per file
MATCH_BY_COLUMN_NAME = NONE;

-- After creation, run DESCRIBE PIPE STREAMING_DB.RAW.EVENTS_PIPE; copy the
-- `notification_channel` (an SQS ARN) and add it as a subscriber of the SNS
-- topic created on the S3 bucket.
--
-- Operational checks:
--   SELECT SYSTEM$PIPE_STATUS('STREAMING_DB.RAW.EVENTS_PIPE');
--   SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
--       TABLE_NAME => 'STREAMING_DB.RAW.RAW_EVENTS',
--       START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())));
