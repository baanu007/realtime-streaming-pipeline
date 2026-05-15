############################################
# Kinesis Data Stream + IAM
############################################

resource "aws_kinesis_stream" "this" {
  name             = var.stream_name
  shard_count      = var.stream_mode == "PROVISIONED" ? var.shard_count : null
  retention_period = var.retention_hours

  stream_mode_details {
    stream_mode = var.stream_mode
  }

  encryption_type = "KMS"
  kms_key_id      = var.kms_key_id

  shard_level_metrics = [
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded",
    "ReadProvisionedThroughputExceeded",
    "IteratorAgeMilliseconds",
  ]

  tags = var.tags
}

# Least-privilege role that producers (EC2, ECS, Lambda) can assume
# to write to the stream.
data "aws_iam_policy_document" "producer_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = var.producer_service_principals
    }
  }
}

resource "aws_iam_role" "producer" {
  name               = "${var.stream_name}-producer"
  assume_role_policy = data.aws_iam_policy_document.producer_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "producer" {
  statement {
    effect = "Allow"
    actions = [
      "kinesis:PutRecord",
      "kinesis:PutRecords",
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
    ]
    resources = [aws_kinesis_stream.this.arn]
  }
}

resource "aws_iam_role_policy" "producer" {
  name   = "${var.stream_name}-producer-policy"
  role   = aws_iam_role.producer.id
  policy = data.aws_iam_policy_document.producer.json
}
