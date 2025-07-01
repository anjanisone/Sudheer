provider "aws" {
  region = var.aws_region
}

resource "aws_cloudwatch_dashboard" "glue_dashboard" {
  dashboard_name = var.dashboard_name

  dashboard_body = jsonencode({
    "widgets": [
        {
            "type": "metric",
            "x": 0,
            "y": 3,
            "width": 11,
            "height": 3,
            "properties": {
                "metrics": [
                    [ { "expression": "METRICS()/3600", "label": "", "id": "e1", "region": var.aws_region } ],
                    [ "Glue", "glue.driver.aggregate.elapsedTime", "Type", "count", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-historical-glue-job-step-4-neptune-${var.environment}", { "id": "m1", "visible": false, "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-neptune-loader-job-${var.environment}", { "id": "m2", "visible": false, "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-historical-glue-job-step-3-opensearch-${var.environment}", { "id": "m3", "visible": false, "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-incr-id-res-job-${var.environment}", { "id": "m4", "visible": false, "region": var.aws_region } ]
                ],
                "view": "singleValue",
                "stacked": false,
                "region": var.aws_region,
                "period": 86400,
                "stat": "Average",
                "yAxis": {
                    "left": {
                        "showUnits": true
                    }
                },
                "singleValueFullPrecision": false,
                "liveData": false,
                "sparkline": false,
                "setPeriodToTimeRange": true,
                "title": "Average Glue Job  Execution Duration ( Hrs )"
            }
        },
        {
            "type": "metric",
            "x": 11,
            "y": 11,
            "width": 13,
            "height": 5,
            "properties": {
                "metrics": [
                    [ "Glue", "glue.driver.workerUtilization", "Type", "gauge", "ObservabilityGroup", "resource_utilization", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-historical-glue-job-step-4-neptune-${var.environment}", { "id": "m1", "region": var.aws_region, "color": "#ff7f0e" } ],
                    [ "...", "udx-cust-mesh-d-neptune-loader-job-${var.environment}", { "id": "m2", "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-historical-glue-job-step-3-opensearch-${var.environment}", { "id": "m3", "region": var.aws_region, "color": "#e377c2" } ],
                    [ "...", "udx-cust-mesh-d-incr-id-res-job-${var.environment}", { "id": "m4", "region": var.aws_region, "color": "#17becf" } ]
                ],
                "view": "gauge",
                "region": var.aws_region,
                "period": 86400,
                "stat": "Average",
                "yAxis": {
                    "left": {
                        "min": 0,
                        "max": 100
                    }
                },
                "setPeriodToTimeRange": true,
                "title": "Glue Worker Utilization ( % )",
                "stacked": false
            }
        },
        {
            "type": "metric",
            "x": 11,
            "y": 0,
            "width": 13,
            "height": 3,
            "properties": {
                "sparkline": false,
                "view": "singleValue",
                "stacked": false,
                "region": var.aws_region,
                "stat": "Sum",
                "period": 86400,
                "metrics": [
                    [ "Glue", "glue.error.ALL", "Type", "count", "ObservabilityGroup", "error", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-historical-glue-job-step-4-neptune-${var.environment}", { "region": var.aws_region } ],
                    [ "...", "ALL", { "region": var.aws_region } ]
                ],
                "title": "Glue Job - Failure Count"
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 0,
            "width": 11,
            "height": 3,
            "properties": {
                "sparkline": false,
                "metrics": [
                    [ "Glue", "glue.succeed.ALL", "Type", "count", "ObservabilityGroup", "error", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-neptune-loader-job-${var.environment}", { "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-historical-glue-job-step-3-opensearch-${var.environment}", { "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-historical-glue-job-step-4-neptune-${var.environment}", { "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-incr-id-res-job-${var.environment}", { "region": var.aws_region } ]
                ],
                "view": "singleValue",
                "stacked": false,
                "region": var.aws_region,
                "stat": "Sum",
                "period": 86400,
                "title": "Glue Job - Success Count"
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 6,
            "width": 11,
            "height": 5,
            "properties": {
                "metrics": [
                    [ { "expression": "100*(m1)", "label": "udx-cust-mesh-d-large-source-etl-glue-job-${var.environment}", "id": "e1" } ],
                    [ { "expression": "100*(m2)", "label": "udx-cust-mesh-d-incr-id-res-job-${var.environment}", "id": "e2", "color": "#2ca02c" } ],
                    [ { "expression": "100*(m3)", "label": "udx-cust-mesh-d-neptune-loader-job-${var.environment}", "id": "e3", "color": "#ff7f0e" } ],
                    [ { "expression": "100*(m4)", "label": "udx-cust-mesh-d-historical-glue-job-step-3-opensearch-${var.environment}", "id": "e4", "color": "#7f7f7f" } ],
                    [ { "expression": "100*(m5)", "label": "udx-cust-mesh-d-historical-glue-job-step-4-neptune-${var.environment}", "id": "e5", "color": "#8c564b" } ],
                    [ "Glue", "glue.ALL.system.cpuSystemLoad", "Type", "gauge", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-large-source-etl-glue-job-${var.environment}", { "id": "m1", "visible": false } ],
                    [ "...", "udx-cust-mesh-d-incr-id-res-job-${var.environment}", { "id": "m2", "visible": false } ],
                    [ "...", "udx-cust-mesh-d-neptune-loader-job-${var.environment}", { "id": "m3", "visible": false } ],
                    [ "...", "udx-cust-mesh-d-historical-glue-job-step-3-opensearch-${var.environment}", { "id": "m4", "visible": false } ],
                    [ "...", "udx-cust-mesh-d-historical-glue-job-step-4-neptune-${var.environment}", { "id": "m5", "visible": false } ]
                ],
                "view": "gauge",
                "region": var.aws_region,
                "yAxis": {
                    "left": {
                        "min": 0,
                        "max": 100
                    }
                },
                "stat": "Average",
                "period": 86400,
                "title": "Average CPU Utilization ( % )"
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 11,
            "width": 11,
            "height": 5,
            "properties": {
                "metrics": [
                    [ { "expression": "m1/1000", "label": "udx-cust-mesh-d-historical-glue-job-step-4-neptune-${var.environment}", "id": "e1" } ],
                    [ { "expression": "m2/1000", "label": "udx-cust-mesh-d-historical-glue-job-step-3-opensearch-${var.environment}", "id": "e2" } ],
                    [ { "expression": "m3/1000", "label": "udx-cust-mesh-d-neptune-loader-job-${var.environment}", "id": "e3" } ],
                    [ { "expression": "m4/1000", "label": "udx-cust-mesh-d-incr-id-res-job-${var.environment}", "id": "e4" } ],
                    [ { "expression": "m5/1000", "label": "udx-cust-mesh-d-large-source-etl-glue-job-${var.environment}", "id": "e5" } ],
                    [ "Glue", "glue.driver.BlockManager.disk.diskSpaceUsed_MB", "Type", "gauge", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-historical-glue-job-step-4-neptune-${var.environment}", { "region": var.aws_region, "id": "m1", "visible": false } ],
                    [ "...", "udx-cust-mesh-d-historical-glue-job-step-3-opensearch-${var.environment}", { "region": var.aws_region, "id": "m2", "visible": false } ],
                    [ "...", "udx-cust-mesh-d-neptune-loader-job-${var.environment}", { "region": var.aws_region, "id": "m3", "visible": false } ],
                    [ "...", "udx-cust-mesh-d-incr-id-res-job-${var.environment}", { "region": var.aws_region, "id": "m4", "visible": false } ],
                    [ "...", "udx-cust-mesh-d-large-source-etl-glue-job-${var.environment}", { "region": var.aws_region, "id": "m5", "visible": false } ]
                ],
                "view": "singleValue",
                "region": var.aws_region,
                "stat": "Average",
                "period": 86400,
                "legend": {
                    "position": "right"
                },
                "setPeriodToTimeRange": true,
                "title": "Average Disk Utilization ( GB )"
            }
        },
        {
            "type": "metric",
            "x": 11,
            "y": 6,
            "width": 13,
            "height": 5,
            "properties": {
                "metrics": [
                    [ { "expression": "100*m1", "label": "udx-cust-mesh-d-incr-id-res-job-${var.environment}", "id": "e1", "color": "#ff7f0e", "region": var.aws_region } ],
                    [ { "expression": "100*m2", "label": "udx-cust-mesh-d-large-source-etl-glue-job-${var.environment}", "id": "e2", "region": var.aws_region } ],
                    [ { "expression": "100*m3", "label": "udx-cust-mesh-d-neptune-loader-job-${var.environment}", "id": "e3", "color": "#ffbb78", "region": var.aws_region } ],
                    [ { "expression": "100*m4", "label": "udx-cust-mesh-d-historical-glue-job-step-3-opensearch-${var.environment}", "id": "e4", "color": "#e377c2", "region": var.aws_region } ],
                    [ { "expression": "100*m5", "label": "udx-cust-mesh-d-historical-glue-job-step-4-neptune-${var.environment}", "id": "e5", "color": "#17becf", "region": var.aws_region } ],
                    [ "Glue", "glue.ALL.jvm.heap.usage", "Type", "gauge", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-incr-id-res-job-${var.environment}", { "id": "m1", "visible": false, "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-large-source-etl-glue-job-${var.environment}", { "id": "m2", "visible": false, "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-neptune-loader-job-${var.environment}", { "id": "m3", "visible": false, "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-historical-glue-job-step-3-opensearch-${var.environment}", { "id": "m4", "visible": false, "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-historical-glue-job-step-4-neptune-${var.environment}", { "id": "m5", "visible": false, "region": var.aws_region } ]
                ],
                "view": "gauge",
                "region": var.aws_region,
                "stat": "Average",
                "period": 86400,
                "yAxis": {
                    "left": {
                        "min": 0,
                        "max": 100
                    }
                },
                "title": "Average Memory Utilization ( % )"
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 16,
            "width": 11,
            "height": 6,
            "properties": {
                "metrics": [
                    [ { "expression": "m1/1000000000", "label": "udx-cust-mesh-d-incr-id-res-job-${var.environment}", "id": "e1", "region": var.aws_region } ],
                    [ { "expression": "m2/1000000000", "label": "udx-cust-mesh-d-historical-glue-job-step-3-opensearch-${var.environment}", "id": "e2", "region": var.aws_region } ],
                    [ { "expression": "m3/1000000000", "label": "udx-cust-mesh-d-historical-glue-job-step-4-neptune-${var.environment}", "id": "e3", "region": var.aws_region } ],
                    [ { "expression": "m4/1000000000", "label": "udx-cust-mesh-d-large-source-etl-glue-job-${var.environment}", "id": "e4", "region": var.aws_region } ],
                    [ { "expression": "m5/1000000000", "label": "udx-cust-mesh-d-neptune-loader-job-${var.environment}", "id": "e5", "region": var.aws_region } ],
                    [ "Glue", "glue.driver.aggregate.bytesRead", "Type", "count", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-incr-id-res-job-${var.environment}", { "id": "m1", "visible": false, "region": var.aws_region } ],
                    [ "Glue", "glue.driver.aggregate.bytesRead", "Type", "count", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-historical-glue-job-step-3-opensearch-${var.environment}", { "id": "m2", "visible": false, "region": var.aws_region } ],
                    [ "Glue", "glue.driver.aggregate.bytesRead", "Type", "count", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-historical-glue-job-step-4-neptune-${var.environment}", { "id": "m3", "visible": false, "region": var.aws_region } ],
                    [ "Glue", "glue.driver.aggregate.bytesRead", "Type", "count", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-large-source-etl-glue-job-${var.environment}", { "id": "m4", "visible": false, "region": var.aws_region } ],
                    [ "Glue", "glue.driver.aggregate.bytesRead", "Type", "count", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-neptune-loader-job-${var.environment}", { "id": "m5", "visible": false, "region": var.aws_region } ]
                ],
                "sparkline": true,
                "view": "timeSeries",
                "region": var.aws_region,
                "stat": "Sum",
                "period": 86400,
                "stacked": true,
                "legend": {
                    "position": "right"
                },
                "yAxis": {
                    "left": {
                        "label": "Data Read ( GB )",
                        "showUnits": false
                    }
                },
                "title": "Average Data Read from all Sources ( GB )"
            }
        },
        {
            "type": "metric",
            "x": 11,
            "y": 16,
            "width": 13,
            "height": 6,
            "properties": {
                "metrics": [
                    [ { "expression": "m1/1000000000", "label": "udx-cust-mesh-d-large-source-etl-glue-job-${var.environment}", "id": "e1", "region": var.aws_region } ],
                    [ { "expression": "m2/1000000000", "label": "udx-cust-mesh-d-incr-id-res-job-${var.environment}", "id": "e2", "region": var.aws_region } ],
                    [ { "expression": "m3/1000000000", "label": "udx-cust-mesh-d-historical-glue-job-step-4-neptune-${var.environment}", "id": "e3", "region": var.aws_region } ],
                    [ "Glue", "glue.driver.bytesWritten", "Type", "gauge", "ObservabilityGroup", "throughput", "Sink", "ALL", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-large-source-etl-glue-job-${var.environment}", { "id": "m1", "visible": false, "period": 86400, "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-incr-id-res-job-${var.environment}", { "id": "m2", "visible": false, "period": 86400, "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-historical-glue-job-step-4-neptune-${var.environment}", { "id": "m3", "visible": false, "period": 86400, "region": var.aws_region } ]
                ],
                "view": "timeSeries",
                "stacked": true,
                "region": var.aws_region,
                "stat": "Average",
                "period": 300,
                "legend": {
                    "position": "right"
                },
                "yAxis": {
                    "left": {
                        "label": "Data Written to all Sinks ( GB )",
                        "showUnits": false
                    }
                },
                "title": "Average Data Written to all Sinks ( GB )"
            }
        },
        {
            "type": "alarm",
            "x": 0,
            "y": 30,
            "width": 24,
            "height": 2,
            "properties": {
                "title": "Glue Job Alarm Status",
                "alarms": [
                    "arn:aws:cloudwatch:us-east-1:481665113918:alarm:udx-cust-mesh-d-neptune-memory-${var.environment}",
                    "arn:aws:cloudwatch:us-east-1:481665113918:alarm:udx-cust-mesh-d-opensearch-cpu-util-${var.environment}",
                    "arn:aws:cloudwatch:us-east-1:481665113918:alarm:udx-cust-mesh-d-neptune-database-capacity-${var.environment}",
                    "arn:aws:cloudwatch:us-east-1:481665113918:alarm:udx-cust-mesh-d-neptune-cpu-utilization-${var.environment}"
                ]
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 22,
            "width": 11,
            "height": 4,
            "properties": {
                "metrics": [
                    [ "Glue", "glue.driver.recordsRead", "Type", "gauge", "ObservabilityGroup", "throughput", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-large-source-etl-glue-job-${var.environment}", "Source", "ALL" ],
                    [ "...", "udx-cust-mesh-d-incr-id-res-job-${var.environment}", ".", "." ],
                    [ "...", "udx-cust-mesh-d-neptune-loader-job-${var.environment}", ".", "." ]
                ],
                "view": "singleValue",
                "stacked": false,
                "region": var.aws_region,
                "title": "Average No.of Records Read across all sources(Count)",
                "setPeriodToTimeRange": true,
                "sparkline": false,
                "period": 86400,
                "stat": "Average"
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 26,
            "width": 11,
            "height": 4,
            "properties": {
                "metrics": [
                    [ "Glue", "glue.driver.filesRead", "Type", "gauge", "ObservabilityGroup", "throughput", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-neptune-loader-job-${var.environment}", "Source", "ALL", { "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-incr-id-res-job-${var.environment}", ".", ".", { "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-large-source-etl-glue-job-${var.environment}", ".", ".", { "region": var.aws_region } ]
                ],
                "view": "singleValue",
                "stacked": false,
                "region": var.aws_region,
                "setPeriodToTimeRange": true,
                "sparkline": false,
                "title": "Average No. of Files Read across all sources (Count)",
                "period": 86400,
                "stat": "Average"
            }
        },
        {
            "type": "metric",
            "x": 11,
            "y": 26,
            "width": 13,
            "height": 4,
            "properties": {
                "metrics": [
                    [ "Glue", "glue.driver.recordsWritten", "Type", "gauge", "ObservabilityGroup", "throughput", "Sink", "ALL", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-incr-id-res-job-${var.environment}", { "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-large-source-etl-glue-job-${var.environment}", { "region": var.aws_region } ]
                ],
                "view": "singleValue",
                "stacked": false,
                "region": var.aws_region,
                "setPeriodToTimeRange": true,
                "sparkline": false,
                "title": "Average No.of Records Written to all sinks (Count)",
                "period": 3600,
                "stat": "Average"
            }
        },
        {
            "type": "metric",
            "x": 11,
            "y": 22,
            "width": 13,
            "height": 4,
            "properties": {
                "metrics": [
                    [ "Glue", "glue.driver.filesWritten", "Type", "gauge", "ObservabilityGroup", "throughput", "Sink", "ALL", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-incr-id-res-job-${var.environment}", { "region": var.aws_region } ],
                    [ "...", "udx-cust-mesh-d-large-source-etl-glue-job-${var.environment}", { "region": var.aws_region } ]
                ],
                "view": "singleValue",
                "stacked": false,
                "region": var.aws_region,
                "setPeriodToTimeRange": true,
                "sparkline": false,
                "title": "Average No. of Files written to all Sinks(Count)",
                "period": 86400,
                "stat": "Average"
            }
        },
        {
            "type": "metric",
            "x": 11,
            "y": 3,
            "width": 13,
            "height": 3,
            "properties": {
                "metrics": [
                    [ "Glue", "glue.driver.skewness.job", "Type", "gauge", "ObservabilityGroup", "job_performance", "JobRunId", "ALL", "JobName", "udx-cust-mesh-d-large-source-etl-glue-job-${var.environment}" ],
                    [ "...", "udx-cust-mesh-d-incr-id-res-job-${var.environment}" ],
                    [ "...", "udx-cust-mesh-d-neptune-loader-job-${var.environment}" ]
                ],
                "view": "singleValue",
                "stacked": false,
                "region": var.aws_region,
                "setPeriodToTimeRange": true,
                "sparkline": false,
                "period": 86400,
                "stat": "Average",
                "title": "Average Glue Job Skewness"
            }
        }
    ]
})
}
