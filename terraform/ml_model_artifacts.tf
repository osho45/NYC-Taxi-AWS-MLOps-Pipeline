locals {
  ml_model_artifacts = {
    "taxi_demand_rf.joblib" = "taxi_demand_rf.joblib"
    "metrics.json"          = "metrics.json"
    "model_metadata.json"   = "model_metadata.json"
  }
}

resource "aws_s3_object" "ml_model_artifacts" {
  for_each = var.upload_model_artifacts ? local.ml_model_artifacts : {}

  bucket = aws_s3_bucket.project["artifacts"].id
  key    = "ml/models/${each.key}"
  source = "${path.module}/../ml/artifacts/${each.value}"
  etag   = filemd5("${path.module}/../ml/artifacts/${each.value}")
}
