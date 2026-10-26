data "archive_file" "data_received_lambda" {
  type = "zip"

  source_dir  = "${path.module}/../lambda/data_received"
  output_path = "${path.module}/data_received_lambda.zip"
}

resource "aws_iam_role" "data_received_lambda" {
  name = "${var.project_name}-data-received-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = "lambda.amazonaws.com"
        }

        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "data_received_lambda_logs" {
  role = aws_iam_role.data_received_lambda.name

  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "data_received_lambda_s3" {
  role = aws_iam_role.data_received_lambda.name

  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

data "aws_iam_policy_document" "data_received_start_workflow" {
  count = var.enable_step_functions ? 1 : 0
  statement {
    sid    = "StartMonthlyMedallionWorkflow"
    effect = "Allow"

    actions = [
      "states:StartExecution",
    ]

    resources = [
      aws_sfn_state_machine.monthly_medallion.arn,
    ]
  }

  statement {
    sid       = "CheckDuplicateMonthlyExecution"
    effect    = "Allow"
    actions   = ["states:DescribeExecution"]
    resources = ["arn:aws:states:${local.region}:${local.account_id}:execution:${var.project_name}-monthly-medallion:*"]
  }
}

resource "aws_iam_role_policy" "data_received_start_workflow" {
  count = var.enable_step_functions ? 1 : 0
  name  = "${var.project_name}-start-monthly-workflow"
  role  = aws_iam_role.data_received_lambda.id

  policy = data.aws_iam_policy_document.data_received_start_workflow[0].json
}

data "aws_iam_policy_document" "data_received_sqs" {
  statement {
    sid    = "ConsumeWorkflowStartQueue"
    effect = "Allow"

    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
    ]

    resources = [
      aws_sqs_queue.workflow_start.arn,
    ]
  }
}

resource "aws_iam_role_policy" "data_received_sqs" {
  name = "${var.project_name}-consume-workflow-start-queue"
  role = aws_iam_role.data_received_lambda.id

  policy = data.aws_iam_policy_document.data_received_sqs.json
}

resource "aws_cloudwatch_log_group" "data_received_lambda" {
  name              = "/aws/lambda/${var.project_name}-data-received"
  retention_in_days = 7
}

resource "aws_lambda_function" "data_received" {
  function_name = "${var.project_name}-data-received"

  role    = aws_iam_role.data_received_lambda.arn
  handler = "lambda_function.lambda_handler"
  runtime = "python3.12"

  filename         = data.archive_file.data_received_lambda.output_path
  source_code_hash = data.archive_file.data_received_lambda.output_base64sha256

  timeout     = 15
  memory_size = 128

  environment {
    variables = {
      EXPECTED_BUCKET       = aws_s3_bucket.project["raw"].bucket
      STATE_MACHINE_ARN     = var.enable_step_functions ? aws_sfn_state_machine.monthly_medallion.arn : ""
      ORCHESTRATION_ENABLED = tostring(var.enable_step_functions)
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.data_received_lambda_logs,
    aws_iam_role_policy_attachment.data_received_lambda_s3,
    aws_iam_role_policy.data_received_sqs,
    aws_iam_role_policy.data_received_start_workflow,
    aws_cloudwatch_log_group.data_received_lambda,
  ]
}
