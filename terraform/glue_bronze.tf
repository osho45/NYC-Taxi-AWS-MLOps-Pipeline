resource "aws_s3_object" "bronze_glue_script" {
  bucket = aws_s3_bucket.project["artifacts"].id
  key    = "glue/jobs/bronze_ingest.py"

  source = "${path.module}/../glue/jobs/bronze_ingest.py"
  etag   = filemd5("${path.module}/../glue/jobs/bronze_ingest.py")
}

resource "aws_glue_job" "bronze_ingest" {
  name     = "${var.project_name}-bronze-ingest"
  role_arn = aws_iam_role.glue_job.arn

  glue_version = "5.0"

  worker_type       = "G.1X"
  number_of_workers = 2

  timeout     = 20
  max_retries = 0

  execution_property {
    max_concurrent_runs = 17
  }

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_object.bronze_glue_script.bucket}/${aws_s3_object.bronze_glue_script.key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--enable-glue-datacatalog"          = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = "true"

    "--TempDir" = "s3://${aws_s3_bucket.project["artifacts"].bucket}/glue/temp/"

    "--LAKE_BUCKET" = aws_s3_bucket.project["lake"].bucket

    "--AUDIT_DATABASE" = aws_glue_catalog_database.medallion["audit"].name

    "--BRONZE_DATABASE" = aws_glue_catalog_database.medallion["bronze"].name

    "--conf" = join(" ", [
      "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
      "--conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.project["lake"].bucket}/warehouse/",
      "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
      "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    ])
  }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy_attachment.glue_s3_full_access,
    aws_s3_object.bronze_glue_script,
  ]
}