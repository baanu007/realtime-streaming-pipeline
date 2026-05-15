# Terraform - Realtime Streaming Pipeline

Modular Terraform that wires Kinesis Data Streams -> Lambda -> Firehose -> S3
and an SNS topic for alerts.

## Modules

| Module             | Purpose                                                |
|--------------------|--------------------------------------------------------|
| `kinesis_stream`   | Encrypted Kinesis Data Stream + producer IAM role      |
| `firehose`         | Firehose delivery stream with Parquet conversion to S3 |
| `lambda_consumer`  | Kinesis-triggered Lambda + DLQ + event source mapping  |
| `sns_topic`        | SNS topic for alerts with email subscriptions          |

## Quick start

```bash
cd infrastructure/terraform

cat > terraform.tfvars <<EOF
project                       = "streamingpipe"
environment                   = "dev"
region                        = "us-east-1"
events_bucket_arn             = "arn:aws:s3:::REPLACE-ME-events-bucket"
stream_processor_package_path = "../../build/stream_processor.zip"
alert_subscribers             = ["you@example.com"]
EOF

terraform init
terraform plan
terraform apply
```

> Account IDs, real bucket names, and ARNs are intentionally **not** stored
> in the repository. Provide them through `terraform.tfvars` (which is in
> `.gitignore`) or your CI secret store.
