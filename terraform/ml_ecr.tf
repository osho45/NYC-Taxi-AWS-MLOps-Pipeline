resource "aws_ecr_repository" "ml_api" {
  name                 = "${var.project_name}-ml-api"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Project     = var.project_name
    Environment = "class"
    Course      = "CDA500"
    ManagedBy   = "Terraform"
  }
}