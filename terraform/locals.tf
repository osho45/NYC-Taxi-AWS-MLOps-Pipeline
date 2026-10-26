locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.region

  bucket_names = {
    raw       = "${var.project_name}-${local.account_id}-${local.region}-raw"
    lake      = "${var.project_name}-${local.account_id}-${local.region}-lake"
    artifacts = "${var.project_name}-${local.account_id}-${local.region}-artifacts"
  }
}