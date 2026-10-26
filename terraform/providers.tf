provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      Course      = "CDA500"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}