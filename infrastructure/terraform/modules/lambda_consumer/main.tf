############################################
# Kinesis-triggered Lambda consumer + DLQ
############################################

data "aws_iam_policy_document" "assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "this" {
  name               = "${var.function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "basic" {
  role       = aws_iam_role.this.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "permissions" {
  statement {
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
      "kinesis:SubscribeToShard",
    ]
    resources = [var.source_stream_arn]
  }

  dynamic "statement" {
    for_each = var.firehose_arn == null ? [] : [1]
    content {
      effect    = "Allow"
      actions   = ["firehose:PutRecord", "firehose:PutRecordBatch"]
      resources = [var.firehose_arn]
    }
  }

  dynamic "statement" {
    for_each = var.sns_topic_arn == null ? [] : [1]
    content {
      effect    = "Allow"
      actions   = ["sns:Publish"]
      resources = [var.sns_topic_arn]
    }
  }

  dynamic "statement" {
    for_each = var.enrichment_table_arn == null ? [] : [1]
    content {
      effect    = "Allow"
      actions   = ["dynamodb:GetItem", "dynamodb:BatchGetItem"]
      resources = [var.enrichment_table_arn]
    }
  }

  statement {
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]
  }
}

resource "aws_iam_role_policy" "permissions" {
  name   = "${var.function_name}-policy"
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.permissions.json
}

# Dead Letter Queue for failed batches
resource "aws_sqs_queue" "dlq" {
  name                       = "${var.function_name}-dlq"
  message_retention_seconds  = 1209600 # 14 days
  visibility_timeout_seconds = 60
  tags                       = var.tags
}

resource "aws_lambda_function" "this" {
  function_name    = var.function_name
  role             = aws_iam_role.this.arn
  handler          = var.handler
  runtime          = var.runtime
  filename         = var.package_path
  source_code_hash = filebase64sha256(var.package_path)
  timeout          = var.timeout
  memory_size      = var.memory_size

  environment {
    variables = var.environment
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.dlq.arn
  }

  tracing_config {
    mode = "Active"
  }

  tags = var.tags
}

resource "aws_lambda_event_source_mapping" "kinesis" {
  event_source_arn                   = var.source_stream_arn
  function_name                      = aws_lambda_function.this.arn
  starting_position                  = "LATEST"
  batch_size                         = var.batch_size
  maximum_batching_window_in_seconds = var.maximum_batching_window
  parallelization_factor             = var.parallelization_factor
  maximum_retry_attempts             = var.maximum_retry_attempts
  bisect_batch_on_function_error     = true
  function_response_types            = ["ReportBatchItemFailures"]

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.dlq.arn
    }
  }
}
