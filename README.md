# ⚡ Realtime Streaming Pipeline

End-to-end streaming pipeline that lands clickstream / order / signup events
in Snowflake within minutes of being generated:

**Producer → Kinesis Data Stream → Lambda (enrich + route) → Firehose → S3 → Snowpipe → Snowflake (Streams + Tasks)**

![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)

---

## What this project actually does

1. A configurable Python **event simulator** publishes synthetic events to
   Amazon **Kinesis Data Streams** using `PutRecords` batching with
   `user_id`-based partition keys.
2. A **Kinesis-triggered Lambda** (`stream_processor`) decodes records,
   enriches them with user metadata from DynamoDB, and routes them to:
   - **Firehose** for batched delivery to S3 (Parquet, dynamic partitioning).
   - **SNS** for high-value-order / fraud alerts.
3. **Snowpipe** auto-ingests every Firehose object into `RAW.RAW_EVENTS`
   (VARIANT landing table) using S3 event notifications via SNS.
4. A Snowflake **Stream** + 1-minute **Task** merges new rows into the typed,
   clustered `CURATED.EVENTS` table.
5. A separate **DLQ Lambda** archives any poison-pill records to a dead-letter
   S3 bucket, partitioned by `dt`/`hh`.
6. A **`HANDLE_LATE_EVENTS`** stored procedure reconciles late-arriving data
   by re-merging from the external table over the same S3 prefix.

The full text-art architecture lives in [`docs/architecture.md`](docs/architecture.md).

---

## Repository layout

```
realtime-streaming-pipeline/
├── src/
│   ├── producers/
│   │   ├── synthetic_data.py        # Faker-based event generator
│   │   └── event_simulator.py       # Kinesis PutRecords driver
│   ├── lambdas/
│   │   ├── stream_processor/        # Kinesis → enrich → Firehose/SNS
│   │   └── dlq_processor/           # SQS DLQ → S3 dead-letter archive
│   └── common/
│       └── kinesis_utils.py         # Kinesis record decoding
├── infrastructure/
│   ├── firehose/
│   │   └── firehose_config.json     # Reference Firehose config
│   └── terraform/
│       ├── main.tf, variables.tf, outputs.tf
│       └── modules/
│           ├── kinesis_stream/
│           ├── firehose/
│           ├── lambda_consumer/
│           └── sns_topic/
├── snowflake/
│   ├── ddl/
│   │   ├── raw_events_table.sql
│   │   ├── staging_external_table.sql
│   │   ├── snowpipe.sql
│   │   ├── streams_tasks.sql
│   │   └── curated_events.sql
│   └── procedures/
│       └── handle_late_events.sql
├── tests/
│   ├── test_event_simulator.py
│   ├── test_stream_processor.py
│   ├── test_dlq_processor.py
│   └── conftest.py
├── data/                            # Small sample payloads
├── docs/architecture.md
├── screenshots/architecture.md
├── .github/workflows/ci.yml
├── requirements.txt
├── requirements-dev.txt
├── pyproject.toml
├── .flake8
├── LICENSE                          # MIT
└── README.md
```

---

## Quick start

### Run the unit tests

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
pytest -v
```

### Run the simulator locally (against a real Kinesis stream)

```bash
export AWS_PROFILE=streaming-dev
python -m src.producers.event_simulator \
    --stream-name streamingpipe-dev-events \
    --rate 200 \
    --duration 60
```

### Deploy the AWS side

```bash
cd infrastructure/terraform
terraform init
terraform apply \
  -var "events_bucket_arn=arn:aws:s3:::REPLACE-ME-events-bucket" \
  -var "stream_processor_package_path=../../build/stream_processor.zip"
```

Build the Lambda package however your CI prefers (zip up `src/` plus
`requirements.txt` deps into `build/stream_processor.zip`).

### Set up the Snowflake side

Run the SQL files in order:

```sql
!source snowflake/ddl/raw_events_table.sql
!source snowflake/ddl/staging_external_table.sql
!source snowflake/ddl/snowpipe.sql            -- then subscribe the SQS ARN
!source snowflake/ddl/curated_events.sql
!source snowflake/ddl/streams_tasks.sql
!source snowflake/procedures/handle_late_events.sql
```

After `CREATE PIPE`, run `DESCRIBE PIPE STREAMING_DB.RAW.EVENTS_PIPE;` and
subscribe the returned SQS ARN to the SNS topic on the events S3 bucket.

---

## Configuration

The stream processor Lambda reads its configuration from environment
variables wired up by Terraform:

| Variable                   | Required | Description                                       |
|----------------------------|----------|---------------------------------------------------|
| `FIREHOSE_DELIVERY_STREAM` | yes      | Name of the Firehose delivery stream.             |
| `SNS_ALERT_TOPIC_ARN`      | yes      | SNS topic for high-value / fraud alerts.          |
| `ENRICHMENT_TABLE`         | no       | DynamoDB table with user metadata.                |
| `HIGH_VALUE_THRESHOLD`     | no       | Dollar threshold for order alerts (default 1000). |
| `LOG_LEVEL`                | no       | Standard Python log level.                        |

---

## Testing

- **`tests/test_event_simulator.py`** — schema, record building, chunking,
  retry-on-partial-failure, and a duration-bounded smoke test using a stub
  Kinesis client.
- **`tests/test_stream_processor.py`** — decode → enrich → route logic,
  partial-batch failures via `batchItemFailures`, env-var validation,
  high-value / fraud routing.
- **`tests/test_dlq_processor.py`** — verifies S3 partitioning and the
  envelope shape archived to the dead-letter bucket.

CI runs `flake8`, `black --check`, and `pytest` on Python 3.10 and 3.11.

---

## Operational notes

- **Latency target**: ~3–5 minutes producer → `CURATED.EVENTS`.
- **Backpressure**: Kinesis ON_DEMAND mode by default; switch to PROVISIONED
  for predictable cost when throughput stabilises.
- **Idempotency**: the Snowflake merge keys on `event_id`, so the same record
  appearing in both Snowpipe and the late-event reconciliation path is safe.
- **Security**: KMS encryption on Kinesis and SNS; least-privilege IAM per
  module; no account IDs, real bucket names, or credentials in source.

---

## License

MIT — see [`LICENSE`](LICENSE).
