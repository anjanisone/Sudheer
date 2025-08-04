provider "aws" {
  region = "us-east-1"
}

resource "aws_cloudwatch_log_metric_filter" "retry_count_filter" {
  name           = "RetryCountMetricFilter"
  log_group_name = "/aws-glue/jobs/error"

  pattern = "msearch inside inner while loop before actual msearch value of retries is # ?retry_count and value of opensearch_client_error_retry_count is # ?opensearch_client_error_retry_count and value of i is ?number"

  metric_transformation {
    name      = "RetryCountMetric"
    namespace = "OpenSearchMonitoring"
    value     = "$retry_count"
  }
}

resource "aws_cloudwatch_metric_alarm" "high_retry_count" {
  alarm_name          = "HighOpenSearchRetryCount"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "RetryCountMetric"
  namespace           = "OpenSearchMonitoring"
  period              = 300
  statistic           = "Maximum"
  threshold           = 5

  alarm_actions = ["arn:aws:sns:us-east-1:123456789012:your-existing-sns-topic"]
  ok_actions    = ["arn:aws:sns:us-east-1:123456789012:your-existing-sns-topic"]
}
