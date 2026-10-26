resource "aws_s3_bucket" "project" {
  for_each = local.bucket_names

  bucket        = each.value
  force_destroy = var.force_destroy_buckets
}

resource "aws_s3_bucket_ownership_controls" "project" {
  for_each = aws_s3_bucket.project

  bucket = each.value.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "project" {
  for_each = aws_s3_bucket.project

  bucket = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "project" {
  for_each = aws_s3_bucket.project

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "project" {
  for_each = aws_s3_bucket.project

  bucket = each.value.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_notification" "raw_eventbridge" {
  bucket      = aws_s3_bucket.project["raw"].id
  eventbridge = true
}