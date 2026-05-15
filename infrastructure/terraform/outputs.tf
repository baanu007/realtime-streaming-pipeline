output "kinesis_stream_name" {
  value = module.kinesis.stream_name
}

output "kinesis_stream_arn" {
  value = module.kinesis.stream_arn
}

output "firehose_delivery_stream_name" {
  value = module.firehose.delivery_stream_name
}

output "stream_processor_function_arn" {
  value = module.stream_processor.function_arn
}

output "stream_processor_dlq_arn" {
  value = module.stream_processor.dlq_arn
}

output "alerts_topic_arn" {
  value = module.alerts_topic.topic_arn
}
