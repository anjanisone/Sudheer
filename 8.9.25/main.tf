terraform {
  required_version = ">= 1.3.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.0" }
  }
}

variable "sns_topic_arn" {
  description = "SNS topic ARN for alarm notifications"
  type        = string
}

variable "db_cluster_id" {
  description = "Neptune DBClusterIdentifier (for cluster-scoped metrics like NCUUtilization/ServerlessDatabaseCapacity)"
  type        = string
}

variable "db_instance_ids" {
  description = "List of Neptune DBInstanceIdentifier values to alarm per-instance metrics (e.g., BufferCacheHitRatio, queue depth)"
  type        = list(string)
}

variable "enable_ncu_alarms" {
  description = "Enable NCUUtilization alarms (Neptune Serverless only)"
  type        = bool
  default     = true
}

# ---------- NCUUtilization (Serverless) ----------
# Sustained >90% means risk of throttling; alarm if AVG > 90% for 30 minutes (6 x 5min periods)
resource "aws_cloudwatch_metric_alarm" "ncu_utilization_high" {
  count               = var.enable_ncu_alarms ? 1 : 0
  alarm_name          = "neptune-${var.db_cluster_id}-NCUUtilization-gt90-30m"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 6
  datapoints_to_alarm = 6
  threshold           = 90
  metric_name         = "NCUUtilization"
  namespace           = "AWS/Neptune"
  period              = 300
  statistic           = "Average"
  treat_missing_data  = "notBreaching"

  dimensions = {
    DBClusterIdentifier = var.db_cluster_id
  }

  alarm_description = "Neptune Serverless NCUUtilization > 90% (avg) for 30m — potential throttling risk."
  alarm_actions     = [var.sns_topic_arn]
  ok_actions        = [var.sns_topic_arn]
}

# ---------- BufferCacheHitRatio ----------
# If cache hit ratio < 99% consistently, queries are hitting disk too often.
# Alarm when AVG < 99 for 15 minutes (3 periods).
resource "aws_cloudwatch_metric_alarm" "buffer_cache_hit_ratio_low" {
  for_each            = toset(var.db_instance_ids)
  alarm_name          = "neptune-${each.key}-BufferCacheHitRatio-lt99-15m"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 3
  datapoints_to_alarm = 3
  threshold           = 99
  metric_name         = "BufferCacheHitRatio"
  namespace           = "AWS/Neptune"
  period              = 300
  statistic           = "Average"
  treat_missing_data  = "breaching"

  dimensions = {
    DBInstanceIdentifier = each.key
  }

  alarm_description = "Neptune BufferCacheHitRatio < 99% (avg) for 15m — cache misses may cause high latency."
  alarm_actions     = [var.sns_topic_arn]
  ok_actions        = [var.sns_topic_arn]
}

# ---------- MainRequestQueuePendingRequests ----------
# WARN: queue depth > 5 for 15 minutes (3 periods).
resource "aws_cloudwatch_metric_alarm" "queue_depth_warn" {
  for_each            = toset(var.db_instance_ids)
  alarm_name          = "neptune-${each.key}-MainRequestQueuePendingRequests-gt5-15m"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  datapoints_to_alarm = 3
  threshold           = 5
  metric_name         = "MainRequestQueuePendingRequests"
  namespace           = "AWS/Neptune"
  period              = 300
  statistic           = "Average"
  treat_missing_data  = "notBreaching"

  dimensions = {
    DBInstanceIdentifier = each.key
  }

  alarm_description = "Neptune main request queue depth > 5 (avg) for 15m — consider more vCPUs / capacity."
  alarm_actions     = [var.sns_topic_arn]
  ok_actions        = [var.sns_topic_arn]
}

# CRIT: queue depth > 100 for 15 minutes (3 periods).
resource "aws_cloudwatch_metric_alarm" "queue_depth_crit" {
  for_each            = toset(var.db_instance_ids)
  alarm_name          = "neptune-${each.key}-MainRequestQueuePendingRequests-gt100-15m"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  datapoints_to_alarm = 3
  threshold           = 100
  metric_name         = "MainRequestQueuePendingRequests"
  namespace           = "AWS/Neptune"
  period              = 300
  statistic           = "Average"
  treat_missing_data  = "notBreaching"

  dimensions = {
    DBInstanceIdentifier = each.key
  }

  alarm_description = "Neptune main request queue depth > 100 (avg) for 15m — requests likely throttled; urgent scale up."
  alarm_actions     = [var.sns_topic_arn]
  ok_actions        = [var.sns_topic_arn]
}
