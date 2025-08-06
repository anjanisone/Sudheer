provider "aws" {
  region = "us-east-1"
}

resource "aws_cloudwatch_log_metric_filter" "neptune_retry_metric_filter" {
  name           = "NeptuneRetryMetricFilter"
  log_group_name = "/aws-glue/jobs/error"

  pattern = "ID_NEPTUNE_PY Retrying for Exception ?exception_type in ?delay seconds. Retry number ?retry of ?max_retry."

  metric_transformation {
    name      = "NeptuneRetryMetric"
    namespace = "NeptuneMonitoring"
    value     = "$retry"
  }
}

resource "aws_cloudwatch_metric_alarm" "high_neptune_retry" {
  alarm_name          = "HighNeptuneRetry"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "NeptuneRetryMetric"
  namespace           = "NeptuneMonitoring"
  period              = 300
  statistic           = "Maximum"
  threshold           = 3

  alarm_actions = ["arn:aws:sns:us-east-1:123456789012:your-existing-sns-topic"]
  ok_actions    = ["arn:aws:sns:us-east-1:123456789012:your-existing-sns-topic"]
}
