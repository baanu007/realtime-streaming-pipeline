variable "stream_name" {
  type        = string
  description = "Name of the Kinesis Data Stream."
}

variable "stream_mode" {
  type        = string
  description = "PROVISIONED or ON_DEMAND."
  default     = "ON_DEMAND"
  validation {
    condition     = contains(["PROVISIONED", "ON_DEMAND"], var.stream_mode)
    error_message = "stream_mode must be PROVISIONED or ON_DEMAND."
  }
}

variable "shard_count" {
  type        = number
  description = "Shard count when stream_mode = PROVISIONED."
  default     = 2
}

variable "retention_hours" {
  type        = number
  description = "Stream retention in hours (24-8760)."
  default     = 24
}

variable "kms_key_id" {
  type        = string
  description = "KMS key id/alias for server-side encryption."
  default     = "alias/aws/kinesis"
}

variable "producer_service_principals" {
  type        = list(string)
  description = "Service principals allowed to assume the producer role."
  default     = ["ec2.amazonaws.com", "lambda.amazonaws.com", "ecs-tasks.amazonaws.com"]
}

variable "tags" {
  type    = map(string)
  default = {}
}
