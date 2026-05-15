variable "topic_name" {
  type = string
}

variable "kms_master_key_id" {
  type    = string
  default = "alias/aws/sns"
}

variable "email_subscribers" {
  type        = list(string)
  description = "Email addresses to subscribe (each will receive a confirmation email)."
  default     = []
}

variable "tags" {
  type    = map(string)
  default = {}
}
