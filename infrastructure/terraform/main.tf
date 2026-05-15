############################################
# Root module: wires Kinesis -> Lambda -> Firehose -> S3
# plus an SNS topic for alerts.
#
# Replace placeholder variables (project, region, bucket_arn, etc.) in a
# *.tfvars file or via -var flags. No real account IDs, ARNs, or bucket
# names live in this repo.
############################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

locals {
  base_name = "${var.project}-${var.environment}"
  tags = merge(
    {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Pipeline    = "realtime-streaming"
    },
    var.tags,
  )
}

module "kinesis" {
  source = "./modules/kinesis_stream"

  stream_name     = "${local.base_name}-events"
  stream_mode     = var.kinesis_stream_mode
  shard_count     = var.kinesis_shard_count
  retention_hours = var.kinesis_retention_hours

  tags = local.tags
}

module "alerts_topic" {
  source = "./modules/sns_topic"

  topic_name        = "${local.base_name}-alerts"
  email_subscribers = var.alert_subscribers

  tags = local.tags
}

module "firehose" {
  source = "./modules/firehose"

  delivery_stream_name   = "${local.base_name}-firehose"
  destination_bucket_arn = var.events_bucket_arn
  glue_database_name     = var.glue_database_name
  glue_table_name        = var.glue_table_name
  region                 = var.region

  tags = local.tags
}

module "stream_processor" {
  source = "./modules/lambda_consumer"

  function_name = "${local.base_name}-stream-processor"
  handler       = "src.lambdas.stream_processor.handler.lambda_handler"
  package_path  = var.stream_processor_package_path

  source_stream_arn    = module.kinesis.stream_arn
  firehose_arn         = module.firehose.delivery_stream_arn
  sns_topic_arn        = module.alerts_topic.topic_arn
  enrichment_table_arn = var.enrichment_table_arn

  environment = {
    FIREHOSE_DELIVERY_STREAM = module.firehose.delivery_stream_name
    SNS_ALERT_TOPIC_ARN      = module.alerts_topic.topic_arn
    ENRICHMENT_TABLE         = var.enrichment_table_name
    HIGH_VALUE_THRESHOLD     = tostring(var.high_value_threshold)
    LOG_LEVEL                = var.log_level
  }

  tags = local.tags
}
