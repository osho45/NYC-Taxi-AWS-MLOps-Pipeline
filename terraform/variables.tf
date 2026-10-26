variable "aws_region" {
  description = "AWS Region in which resources will be deployed."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short lowercase project name used in resource names."
  type        = string
  default     = "nyc-taxi-ml"

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.project_name))
    error_message = "project_name may contain only lowercase letters, numbers, and hyphens."
  }
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "class"
}

variable "force_destroy_buckets" {
  description = "Allow Terraform to delete non-empty buckets during classroom teardown."
  type        = bool
  default     = true
}
variable "enable_step_functions" {
  description = "Enable SQS-triggered Step Functions orchestration."
  type        = bool
  default     = false
}

variable "upload_model_artifacts" {
  description = "Manage and upload local model artifacts to S3. Keep false when existing cloud objects must be preserved."
  type        = bool
  default     = false
}
