output "aws_account_id" {
  description = "AWS account receiving the deployment."
  value       = data.aws_caller_identity.current.account_id
}

output "aws_region" {
  description = "AWS deployment Region."
  value       = data.aws_region.current.region
}

output "raw_bucket_name" {
  description = "Bucket used for incoming raw taxi files."
  value       = aws_s3_bucket.project["raw"].bucket
}

output "lake_bucket_name" {
  description = "Bucket used for the Iceberg data lake."
  value       = aws_s3_bucket.project["lake"].bucket
}

output "artifacts_bucket_name" {
  description = "Bucket used for scripts, workflows, models, and deployment artifacts."
  value       = aws_s3_bucket.project["artifacts"].bucket
}

output "bucket_names" {
  description = "All project S3 buckets."
  value = {
    for name, bucket in aws_s3_bucket.project :
    name => bucket.bucket
  }
}

output "data_received_lambda_name" {
  description = "Lambda function acknowledging newly uploaded raw files."
  value       = aws_lambda_function.data_received.function_name
}

output "data_received_log_group" {
  description = "CloudWatch log group for the acknowledgement Lambda."
  value       = aws_cloudwatch_log_group.data_received_lambda.name
}

output "raw_object_event_rule_name" {
  description = "EventBridge rule for newly created raw taxi objects."
  value       = aws_cloudwatch_event_rule.raw_taxi_object_created.name
}

output "glue_job_role_name" {
  description = "IAM role used by the AWS Glue Spark jobs."
  value       = aws_iam_role.glue_job.name
}

output "glue_job_role_arn" {
  description = "ARN of the IAM role used by AWS Glue Spark jobs."
  value       = aws_iam_role.glue_job.arn
}

output "glue_database_names" {
  description = "Glue Data Catalog databases for the medallion architecture."
  value = {
    for layer, database in aws_glue_catalog_database.medallion :
    layer => database.name
  }
}

output "athena_results_s3_uri" {
  description = "Suggested S3 location for Athena query results."
  value       = "s3://${aws_s3_bucket.project["artifacts"].bucket}/athena-results/"
}

output "bronze_glue_job_name" {
  description = "AWS Glue job that ingests raw taxi files into Bronze Iceberg."
  value       = aws_glue_job.bronze_ingest.name
}

output "silver_glue_job_name" {
  description = "AWS Glue job that validates and publishes Silver taxi data."
  value       = aws_glue_job.silver_publish.name
}

output "gold_glue_job_name" {
  description = "AWS Glue job that builds the Gold taxi tables."
  value       = aws_glue_job.gold_build.name
}

output "step_functions_state_machine_arn" {
  description = "ARN of the monthly Step Functions medallion state machine."
  value       = aws_sfn_state_machine.monthly_medallion.arn
}

output "step_functions_log_group" {
  description = "CloudWatch log group for monthly Step Functions executions."
  value       = aws_cloudwatch_log_group.step_functions.name
}

output "mwaa_serverless_execution_role_arn" {
  description = "IAM execution role used by the MWAA Serverless workflow."
  value       = aws_iam_role.mwaa_workflow.arn
}

output "mwaa_serverless_log_group" {
  description = "CloudWatch log group for MWAA Serverless workflow runs."
  value       = aws_cloudwatch_log_group.mwaa_workflow.name
}

output "workflow_start_queue_url" {
  description = "FIFO queue buffering monthly workflow-start requests."
  value       = aws_sqs_queue.workflow_start.url
}

output "workflow_start_queue_arn" {
  description = "ARN of the FIFO workflow-start queue."
  value       = aws_sqs_queue.workflow_start.arn
}

output "workflow_start_dlq_url" {
  description = "Dead-letter queue for failed workflow-start messages."
  value       = aws_sqs_queue.workflow_start_dlq.url
}

output "ml_api_ecr_repository_url" {
  description = "ECR repository URL for the ML prediction API."
  value       = aws_ecr_repository.ml_api.repository_url
}

output "ml_api_url" {
  description = "Public URL for the ML prediction API."
  value       = aws_apigatewayv2_api.ml_api.api_endpoint
}

output "demand_history_table_name" {
  description = "DynamoDB table containing historical taxi demand."
  value       = aws_dynamodb_table.taxi_demand_history.name
}
