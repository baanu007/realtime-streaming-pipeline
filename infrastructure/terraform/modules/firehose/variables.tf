variable "delivery_stream_name" {
  type = string
}

variable "destination_bucket_arn" {
  type        = string
  description = "ARN of the events landing S3 bucket."
}

variable "s3_prefix" {
  type    = string
  default = "events/!{partitionKeyFromQuery:event_type}/dt=!{timestamp:yyyy-MM-dd}/hh=!{timestamp:HH}/"
}

variable "error_prefix" {
  type    = string
  default = "errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd}/hh=!{timestamp:HH}/"
}

variable "buffer_size_mb" {
  type    = number
  default = 64
}

variable "buffer_interval_seconds" {
  type    = number
  default = 60
}

variable "transform_lambda_arn" {
  type        = string
  description = "Optional Firehose data transformation Lambda ARN."
  default     = null
}

variable "glue_database_name" {
  type = string
}

variable "glue_table_name" {
  type    = string
  default = "raw_events"
}

variable "region" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
