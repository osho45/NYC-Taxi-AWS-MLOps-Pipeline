# ---------------------------------------------------------
# Find the Docker image we pushed to ECR
# ---------------------------------------------------------

data "aws_ecr_image" "ml_api" {
  repository_name = aws_ecr_repository.ml_api.name
  image_tag       = "range-v3"
}


# ---------------------------------------------------------
# Lambda IAM Role
# ---------------------------------------------------------

resource "aws_iam_role" "ml_api_lambda" {
  name = "${var.project_name}-ml-api-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = "lambda.amazonaws.com"
        }

        Action = "sts:AssumeRole"
      }
    ]
  })
}


# Allow Lambda to write CloudWatch logs
resource "aws_iam_role_policy_attachment" "ml_api_lambda_logs" {
  role       = aws_iam_role.ml_api_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}


# Allow Lambda to download the ML model from S3
resource "aws_iam_role_policy" "ml_api_s3" {
  name = "${var.project_name}-ml-api-data-access"
  role = aws_iam_role.ml_api_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Action = [
          "dynamodb:Query",
          "dynamodb:GetItem"
        ]

        Resource = aws_dynamodb_table.taxi_demand_history.arn
      },

      {
        Effect = "Allow"

        Action = [
          "s3:GetObject"
        ]

        Resource = [
          "${aws_s3_bucket.project["artifacts"].arn}/ml/models/*"
        ]
      }
    ]
  })
}


# ---------------------------------------------------------
# Lambda
# ---------------------------------------------------------

resource "aws_lambda_function" "ml_api" {
  function_name = "${var.project_name}-ml-api"

  package_type = "Image"

  image_uri = "${aws_ecr_repository.ml_api.repository_url}@${data.aws_ecr_image.ml_api.image_digest}"

  role = aws_iam_role.ml_api_lambda.arn

  memory_size = 1024
  timeout     = 30

  environment {
    variables = {
      ARTIFACTS_BUCKET     = aws_s3_bucket.project["artifacts"].bucket
      DEMAND_HISTORY_TABLE = aws_dynamodb_table.taxi_demand_history.name
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.ml_api_lambda_logs,
    aws_iam_role_policy.ml_api_s3,
    aws_s3_object.ml_model_artifacts,
  ]
}


# ---------------------------------------------------------
# API Gateway HTTP API
# ---------------------------------------------------------

resource "aws_apigatewayv2_api" "ml_api" {
  name          = "${var.project_name}-ml-api"
  protocol_type = "HTTP"

  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["POST"]
    allow_headers = ["content-type"]
  }
}
resource "aws_apigatewayv2_route" "ml_range" {
  api_id = aws_apigatewayv2_api.ml_api.id

  route_key = "GET /range"

  target = "integrations/${aws_apigatewayv2_integration.ml_api.id}"
}

resource "aws_apigatewayv2_integration" "ml_api" {
  api_id = aws_apigatewayv2_api.ml_api.id

  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.ml_api.invoke_arn
  payload_format_version = "2.0"
}


resource "aws_apigatewayv2_route" "predict" {
  api_id = aws_apigatewayv2_api.ml_api.id

  route_key = "POST /predict"
  target    = "integrations/${aws_apigatewayv2_integration.ml_api.id}"
}


resource "aws_apigatewayv2_stage" "default" {
  api_id = aws_apigatewayv2_api.ml_api.id

  name        = "$default"
  auto_deploy = true
}


# Allow API Gateway to invoke Lambda
resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ml_api.function_name
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_apigatewayv2_api.ml_api.execution_arn}/*/*"
}
