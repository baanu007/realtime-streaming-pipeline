############################################
# SNS topic for streaming alerts
############################################

resource "aws_sns_topic" "this" {
  name              = var.topic_name
  kms_master_key_id = var.kms_master_key_id
  tags              = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  for_each  = toset(var.email_subscribers)
  topic_arn = aws_sns_topic.this.arn
  protocol  = "email"
  endpoint  = each.value
}
