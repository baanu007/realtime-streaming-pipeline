# Firehose Delivery Stream

`firehose_config.json` is a reference payload for `aws firehose create-delivery-stream`.
The matching Terraform module under `../terraform/modules/firehose/` is the
preferred way to deploy this in any real environment - the JSON file is here
so reviewers can see, in one place, every knob we tune.

## Buffer hints

- **SizeInMBs: 64** - large enough to land Snowpipe-friendly file sizes.
- **IntervalInSeconds: 60** - keeps end-to-end latency under ~2 minutes.

## Dynamic partitioning

Firehose extracts `event_type` from each record (JQ `{event_type: .event_type}`)
and lays files out as `events/<event_type>/dt=YYYY-MM-DD/hh=HH/`. Snowpipe's
external stage points at the same prefix so partition pruning works in both
S3 and Snowflake.

## Format conversion

Records arrive as newline-delimited JSON and are converted to Snappy-compressed
Parquet using the schema registered in the Glue Data Catalog
(`<GLUE_DATABASE>.raw_events`). Parquet keeps Snowpipe ingest cheap and
makes ad-hoc Athena queries on the same data possible.

## Error output

Anything Firehose cannot transform lands under `errors/` in the same bucket so
you have a clear audit trail without polluting the happy path.
