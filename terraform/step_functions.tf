locals {
  step_functions_definition = templatefile(
    "${path.module}/../workflows/medallion_monthly.asl.json.tftpl",
    {
      bronze_job_name = aws_glue_job.bronze_ingest.name
      silver_job_name = aws_glue_job.silver_publish.name
      gold_job_name   = aws_glue_job.gold_build.name
    }
  )
}

data "aws_iam_policy_document" "step_functions_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:aws:states:${local.region}:${local.account_id}:stateMachine:${var.project_name}-monthly-medallion"]
    }
  }
}

resource "aws_iam_role" "step_functions" {
  name               = "${var.project_name}-step-functions-role"
  assume_role_policy = data.aws_iam_policy_document.step_functions_assume_role.json
}

data "aws_iam_policy_document" "step_functions" {
  statement {
    sid    = "RunMonthlyGlueJobs"
    effect = "Allow"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
    ]
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:job/${aws_glue_job.bronze_ingest.name}",
      "arn:aws:glue:${local.region}:${local.account_id}:job/${aws_glue_job.silver_publish.name}",
      "arn:aws:glue:${local.region}:${local.account_id}:job/${aws_glue_job.gold_build.name}",
    ]
  }

  statement {
    sid    = "DeliverExecutionLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "step_functions" {
  name   = "${var.project_name}-run-monthly-glue-jobs"
  role   = aws_iam_role.step_functions.id
  policy = data.aws_iam_policy_document.step_functions.json
}

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/vendedlogs/states/${var.project_name}-monthly-medallion"
  retention_in_days = 7
}

resource "aws_sfn_state_machine" "monthly_medallion" {
  name       = "${var.project_name}-monthly-medallion"
  role_arn   = aws_iam_role.step_functions.arn
  type       = "STANDARD"
  definition = local.step_functions_definition

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  depends_on = [aws_iam_role_policy.step_functions]
}
