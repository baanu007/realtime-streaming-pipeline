############################################
# Firehose delivery stream -> S3 (Parquet)
############################################

data "aws_iam_policy_document" "firehose_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${var.delivery_stream_name}-role"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "firehose" {
  statement {
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]
    resources = [
      var.destination_bucket_arn,
      "${var.destination_bucket_arn}/*",
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "glue:GetTable",
      "glue:GetTableVersion",
      "glue:GetTableVersions",
    ]
    resources = ["*"]
  }

  statement {
    effect    = "Allow"
    actions   = ["logs:PutLogEvents", "logs:CreateLogStream"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "${var.delivery_stream_name}-policy"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose.json
}

resource "aws_kinesis_firehose_delivery_stream" "this" {
  name        = var.delivery_stream_name
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = var.destination_bucket_arn
    prefix              = var.s3_prefix
    error_output_prefix = var.error_prefix
    buffering_size      = var.buffer_size_mb
    buffering_interval  = var.buffer_interval_seconds
    compression_format  = "UNCOMPRESSED" # parquet is already compressed

    dynamic "processing_configuration" {
      for_each = var.transform_lambda_arn == null ? [] : [1]
      content {
        enabled = true
        processors {
          type = "Lambda"
          parameters {
            parameter_name  = "LambdaArn"
            parameter_value = var.transform_lambda_arn
          }
        }
      }
    }

    dynamic_partitioning_configuration {
      enabled = true
    }

    data_format_conversion_configuration {
      input_format_configuration {
        deserializer {
          open_x_json_ser_de {
            case_insensitive = true
          }
        }
      }
      output_format_configuration {
        serializer {
          parquet_ser_de {
            compression = "SNAPPY"
          }
        }
      }
      schema_configuration {
        role_arn      = aws_iam_role.firehose.arn
        database_name = var.glue_database_name
        table_name    = var.glue_table_name
        region        = var.region
      }
    }

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = "/aws/kinesisfirehose/${var.delivery_stream_name}"
      log_stream_name = "S3Delivery"
    }
  }

  tags = var.tags
}
