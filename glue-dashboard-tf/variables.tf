variable "aws_region" {
  description = "AWS region to deploy resources in"
  type        = string
  default     = "us-east-1"
}

variable "dashboard_name" {
  description = "Base name of the CloudWatch dashboard"
  type        = string
  default     = "GlueJobsDashboard"
}

variable "environment" {
  description = "Deployment environment (e.g., dev, qa, preprod, prod)"
  type        = string
  default     = "dev"
}
