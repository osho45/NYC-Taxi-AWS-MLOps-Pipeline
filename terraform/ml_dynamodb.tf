resource "aws_dynamodb_table" "taxi_demand_history" {
  name         = "${var.project_name}-demand-history"
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "pickup_location_id"
  range_key = "pickup_hour_ts"

  attribute {
    name = "pickup_location_id"
    type = "N"
  }

  attribute {
    name = "pickup_hour_ts"
    type = "S"
  }

  tags = {
    Project     = var.project_name
    Environment = "class"
    Course      = "CDA500"
    ManagedBy   = "Terraform"
  }
}
