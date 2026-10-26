resource "aws_cloudwatch_event_rule" "raw_taxi_object_created" {
  name = "${var.project_name}-raw-taxi-object-created"

  description = "Routes newly created Hive-partitioned Yellow Taxi Parquet objects from the raw S3 bucket to the workflow-start FIFO queue."

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    account     = [data.aws_caller_identity.current.account_id]
    region      = [data.aws_region.current.region]

    detail = {
      bucket = {
        name = [aws_s3_bucket.project["raw"].bucket]
      }

      object = {
        key = [
          {
            wildcard = "yellow/year=*/month=*/*.parquet"
          }
        ]
      }
    }
  })
}

resource "aws_cloudwatch_event_target" "workflow_start_queue" {
  rule      = aws_cloudwatch_event_rule.raw_taxi_object_created.name
  target_id = "WorkflowStartQueue"
  arn       = aws_sqs_queue.workflow_start.arn

  sqs_target {
    message_group_id = "monthly-workflows"
  }

  depends_on = [
    aws_sqs_queue_policy.workflow_start
  ]
}