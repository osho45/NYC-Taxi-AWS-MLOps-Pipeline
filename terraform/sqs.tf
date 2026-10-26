resource "aws_sqs_queue" "workflow_start_dlq" {
  name = "${var.project_name}-workflow-start-dlq.fifo"

  fifo_queue                  = true
  content_based_deduplication = true
  message_retention_seconds   = 1209600
  sqs_managed_sse_enabled     = true
}

resource "aws_sqs_queue" "workflow_start" {
  name = "${var.project_name}-workflow-start.fifo"

  fifo_queue                  = true
  content_based_deduplication = true

  visibility_timeout_seconds = 120
  message_retention_seconds  = 1209600
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.workflow_start_dlq.arn
    maxReceiveCount     = 100
  })
}

data "aws_iam_policy_document" "workflow_start_queue" {
  statement {
    sid    = "AllowEventBridgeSendMessage"
    effect = "Allow"

    principals {
      type = "Service"

      identifiers = [
        "events.amazonaws.com"
      ]
    }

    actions = [
      "sqs:SendMessage"
    ]

    resources = [
      aws_sqs_queue.workflow_start.arn
    ]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"

      values = [
        aws_cloudwatch_event_rule.raw_taxi_object_created.arn
      ]
    }
  }
}

resource "aws_sqs_queue_policy" "workflow_start" {
  queue_url = aws_sqs_queue.workflow_start.url
  policy    = data.aws_iam_policy_document.workflow_start_queue.json
}

resource "aws_lambda_event_source_mapping" "workflow_start" {
  event_source_arn = aws_sqs_queue.workflow_start.arn
  function_name    = aws_lambda_function.data_received.arn

  batch_size = 1
  enabled    = var.enable_step_functions

  depends_on = [
    aws_iam_role_policy.data_received_sqs
  ]
}
