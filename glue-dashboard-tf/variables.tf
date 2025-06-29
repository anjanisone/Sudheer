variable "aws_region" {
  description = "AWS region where the resources will be created"
  type        = string
  default     = "us-east-1"
}

variable "dashboard_name" {
  description = "The name of the CloudWatch Dashboard"
  type        = string
  default     = "GlueJobsDashboard"
}
