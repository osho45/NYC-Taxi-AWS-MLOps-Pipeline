locals {
  mwaa_workflow_name = "${var.project_name}-monthly-medallion"

  mwaa_workflow_definition = templatefile(
    "${path.module}/../workflows/medallion_monthly.yaml.tftpl",
    {
      bronze_job_name = aws_glue_job.bronze_ingest.name
      silver_job_name = aws_glue_job.silver_publish.name
      gold_job_name   = aws_glue_job.gold_build.name
    }
  )
}

resource "aws_s3_object" "mwaa_workflow_definition" {
  bucket = aws_s3_bucket.project["artifacts"].id
  key    = "workflows/medallion_monthly.yaml"

  content      = local.mwaa_workflow_definition
  content_type = "application/yaml"
  etag         = md5(local.mwaa_workflow_definition)
}

data "aws_iam_policy_document" "mwaa_workflow_assume_role" {
  statement {
    sid     = "AllowMWAAServerless"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type = "Service"

      identifiers = [
        "airflow-serverless.amazonaws.com"
      ]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"

      values = [
        local.account_id
      ]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"

      values = [
        "arn:aws:airflow-serverless:${local.region}:${local.account_id}:*"
      ]
    }
  }
}

resource "aws_iam_role" "mwaa_workflow" {
  name = "${var.project_name}-mwaa-serverless-workflow"

  assume_role_policy = data.aws_iam_policy_document.mwaa_workflow_assume_role.json
}

data "aws_iam_policy_document" "mwaa_workflow" {
  statement {
    sid    = "RunGlueJobs"
    effect = "Allow"

    actions = [
      "glue:GetJob",
      "glue:GetJobs",
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
    sid    = "ReadGlueJobLogs"
    effect = "Allow"

    actions = [
      "logs:FilterLogEvents",
    ]

    resources = [
      "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/jobs/output:*",
      "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/jobs/error:*",
    ]
  }

  statement {
    sid    = "ReadWorkflowBucket"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]

    resources = [
      aws_s3_bucket.project["artifacts"].arn
    ]
  }

  statement {
    sid    = "ReadWorkflowDefinition"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]

    resources = [
      "${aws_s3_bucket.project["artifacts"].arn}/workflows/*"
    ]
  }

  statement {
    sid    = "WriteWorkflowLogs"
    effect = "Allow"

    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]

    resources = [
      "${aws_cloudwatch_log_group.mwaa_workflow.arn}:*"
    ]
  }


}

resource "aws_iam_role_policy" "mwaa_workflow" {
  name = "${var.project_name}-mwaa-serverless-workflow"
  role = aws_iam_role.mwaa_workflow.id

  policy = data.aws_iam_policy_document.mwaa_workflow.json
}

resource "aws_cloudwatch_log_group" "mwaa_workflow" {
  name = "/aws/mwaa-serverless/${local.mwaa_workflow_name}"

  retention_in_days = 7
}
