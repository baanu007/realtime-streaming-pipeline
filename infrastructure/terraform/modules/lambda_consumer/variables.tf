variable "function_name" {
  type = string
}

variable "handler" {
  type = string
}

variable "runtime" {
  type    = string
  default = "python3.11"
}

variable "package_path" {
  type        = string
  description = "Path to the zipped Lambda deployment package."
}

variable "timeout" {
  type    = number
  default = 60
}

variable "memory_size" {
  type    = number
  default = 512
}

variable "source_stream_arn" {
  type = string
}

variable "firehose_arn" {
  type    = string
  default = null
}

variable "sns_topic_arn" {
  type    = string
  default = null
}

variable "enrichment_table_arn" {
  type    = string
  default = null
}

variable "batch_size" {
  type    = number
  default = 200
}

variable "maximum_batching_window" {
  type    = number
  default = 5
}

variable "parallelization_factor" {
  type    = number
  default = 4
}

variable "maximum_retry_attempts" {
  type    = number
  default = 3
}

variable "environment" {
  type    = map(string)
  default = {}
}

variable "tags" {
  type    = map(string)
  default = {}
}
