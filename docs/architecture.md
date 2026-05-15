# Architecture

```
┌──────────────┐   PutRecords   ┌──────────────────┐
│ event_       │ ─────────────▶ │ Kinesis Data     │
│ simulator.py │                │ Stream           │
└──────────────┘                └─────────┬────────┘
                                          │ event source mapping
                                          ▼
                                ┌──────────────────┐
                                │ Stream Processor │
                                │ Lambda           │
                                │  • base64 decode │
                                │  • DynamoDB      │
                                │    enrichment    │
                                │  • route         │
                                └────┬────────┬────┘
            high-value / fraud      │        │   all valid events
                       ┌────────────┘        └────────────┐
                       ▼                                  ▼
              ┌────────────────┐                ┌────────────────────┐
              │ SNS Alerts     │                │ Firehose Delivery  │
              │ Topic          │                │ Stream             │
              └────────────────┘                │  • Parquet         │
                                                │  • dynamic         │
                                                │    partitioning    │
                                                └─────────┬──────────┘
                                                          │
                                                          ▼
                                                ┌────────────────────┐
                                                │ S3 events/         │
                                                │ event_type/dt/hh/  │
                                                └─────────┬──────────┘
                              S3 Event Notification (SNS)│
                                                          ▼
                                                ┌────────────────────┐
                                                │ Snowpipe           │
                                                │ (auto-ingest)      │
                                                └─────────┬──────────┘
                                                          ▼
                                                ┌────────────────────┐
                                                │ RAW.RAW_EVENTS     │
                                                │ (VARIANT)          │
                                                └─────────┬──────────┘
                                                          │ Stream
                                                          ▼
                                                ┌────────────────────┐
                                                │ TASK (1 min)       │
                                                │ MERGE_RAW_TO_      │
                                                │ CURATED            │
                                                └─────────┬──────────┘
                                                          ▼
                                                ┌────────────────────┐
                                                │ CURATED.EVENTS     │
                                                │ (typed, clustered) │
                                                └────────────────────┘
```

## End-to-end latency budget

| Stage                          | Typical |
|--------------------------------|---------|
| Producer -> Kinesis            | < 100 ms |
| Kinesis -> Lambda invocation   | 1-5 s   |
| Lambda -> Firehose buffer flush | 60-90 s |
| Firehose -> S3 object          | included above |
| Snowpipe auto-ingest           | 30-90 s |
| Stream + Task MERGE            | 60 s    |

End-to-end: **~3-5 minutes** for typed rows to appear in `CURATED.EVENTS`,
with alerts firing within seconds of the Lambda invocation.

## Failure handling

- **Producer side**: `PutRecords` partial failures are retried with
  exponential backoff up to 3 times.
- **Lambda side**: uses `ReportBatchItemFailures` so only failed records are
  retried; persistent failures go to an SQS DLQ.
- **DLQ processor**: archives every poison-pill payload to
  `s3://<dlq-bucket>/streaming-dlq/dt=.../hh=.../` for offline analysis.
- **Snowflake side**: `ON_ERROR = 'SKIP_FILE_10%'` tolerates small amounts of
  corrupt data per file; `HANDLE_LATE_EVENTS` reconciles via the external
  table when downstream reports flag a mismatch.

## Security notes

- KMS encryption on Kinesis and SNS.
- Least-privilege IAM roles per module (producer, Firehose, Lambda).
- No real account IDs, ARNs, or bucket names are checked into the repo;
  everything is parameterised through Terraform variables.
