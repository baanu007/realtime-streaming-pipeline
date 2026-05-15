variable "project" {
  type        = string
  description = "Short project name used as a prefix for every resource."
  default     = "streamingpipe"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "tags" {
  type    = map(string)
  default = {}
}

# Kinesis
variable "kinesis_stream_mode" {
  type    = string
  default = "ON_DEMAND"
}

variable "kinesis_shard_count" {
  type    = number
  default = 2
}

variable "kinesis_retention_hours" {
  type    = number
  default = 24
}

# Firehose / S3 / Glue
variable "events_bucket_arn" {
  type        = string
  description = "S3 bucket ARN where Firehose lands events. Provide via tfvars."
}

variable "glue_database_name" {
  type    = string
  default = "streaming_events"
}

variable "glue_table_name" {
  type    = string
  default = "raw_events"
}

# Lambda
variable "stream_processor_package_path" {
  type        = string
  description = "Path to the zipped Lambda deployment package."
}

variable "enrichment_table_arn" {
  type        = string
  description = "DynamoDB table ARN used for user enrichment (optional)."
  default     = null
}

variable "enrichment_table_name" {
  type    = string
  default = ""
}

variable "high_value_threshold" {
  type    = number
  default = 1000
}

variable "log_level" {
  type    = string
  default = "INFO"
}

# SNS
variable "alert_subscribers" {
  type        = list(string)
  description = "Email addresses to receive streaming alerts."
  default     = []
}
