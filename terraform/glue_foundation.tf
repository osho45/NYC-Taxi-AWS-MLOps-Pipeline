locals {
  glue_database_prefix = replace(var.project_name, "-", "_")

  glue_databases = {
    audit      = "${local.glue_database_prefix}_audit"
    bronze     = "${local.glue_database_prefix}_bronze"
    silver     = "${local.glue_database_prefix}_silver"
    quarantine = "${local.glue_database_prefix}_quarantine"
    gold       = "${local.glue_database_prefix}_gold"
  }
}

resource "aws_iam_role" "glue_job" {
  name = "AWSGlueServiceRole-${var.project_name}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = "glue.amazonaws.com"
        }

        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role = aws_iam_role.glue_job.name

  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_s3_full_access" {
  role = aws_iam_role.glue_job.name

  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}
resource "aws_iam_role_policy" "glue_dynamodb_access" {
  name = "${var.project_name}-glue-dynamodb-access"
  role = aws_iam_role.glue_job.name

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Action = [
          "dynamodb:BatchWriteItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:GetItem"
        ]

        Resource = aws_dynamodb_table.taxi_demand_history.arn
      }
    ]
  })
}

resource "aws_glue_catalog_database" "medallion" {
  for_each = local.glue_databases

  name = each.value

  description = "${title(each.key)} database for the NYC taxi medallion pipeline."
}