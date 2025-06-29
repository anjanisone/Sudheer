provider "aws" {
  region = var.aws_region
}

resource "aws_cloudwatch_dashboard" "glue_dashboard" {
  dashboard_name = var.dashboard_name

  dashboard_body = jsonencode({
    widgets = [

      ## 1. CPU Efficiency Widget
      {
        "type": "metric",
        "x": 0, "y": 0, "width": 12, "height": 6,
        "properties": {
          "title": "Glue CPU Efficiency",
          "view": "timeSeries",
          "stacked": false,
          "region": var.aws_region,
          "metrics": [
            [ "Glue", "cpu.utilization", "JobName", "ALL", { "stat": "Average" } ]
          ],
          "period": 300,
          "stat": "Average",
          "yAxis": {
            "left": { "label": "CPU %" }
          }
        }
      },

      ## 2. Memory Utilization Widget
      {
        "type": "metric",
        "x": 12, "y": 0, "width": 12, "height": 6,
        "properties": {
          "title": "Glue Memory Utilization",
          "view": "timeSeries",
          "stacked": false,
          "region": var.aws_region,
          "metrics": [
            [ "Glue", "memory.utilization", "JobName", "ALL", { "stat": "Average" } ]
          ],
          "period": 300,
          "stat": "Average",
          "yAxis": {
            "left": { "label": "Memory %" }
          }
        }
      },

      ## 3. Failed Jobs
      {
        "type": "metric",
        "x": 0, "y": 6, "width": 12, "height": 6,
        "properties": {
          "title": "Failed Glue Jobs",
          "view": "timeSeries",
          "stacked": false,
          "region": var.aws_region,
          "metrics": [
            [ "Glue", "glue.job.failed.count", "JobName", "ALL", { "stat": "Sum" } ]
          ],
          "period": 300,
          "stat": "Sum",
          "yAxis": {
            "left": { "label": "Count" }
          }
        }
      },

      ## 4. Successful Jobs
      {
        "type": "metric",
        "x": 12, "y": 6, "width": 12, "height": 6,
        "properties": {
          "title": "Successful Glue Jobs",
          "view": "timeSeries",
          "stacked": false,
          "region": var.aws_region,
          "metrics": [
            [ "Glue", "glue.job.succeeded.count", "JobName", "ALL", { "stat": "Sum" } ]
          ],
          "period": 300,
          "stat": "Sum",
          "yAxis": {
            "left": { "label": "Count" }
          }
        }
      },

      ## 5. Running Jobs
      {
        "type": "metric",
        "x": 0, "y": 12, "width": 24, "height": 6,
        "properties": {
          "title": "Running Glue Jobs",
          "view": "timeSeries",
          "stacked": false,
          "region": var.aws_region,
          "metrics": [
            [ "Glue", "glue.job.running.count", "JobName", "ALL", { "stat": "Average" } ]
          ],
          "period": 300,
          "stat": "Average",
          "yAxis": {
            "left": { "label": "Running Count" }
          }
        }
      }
    ]
  })
}
