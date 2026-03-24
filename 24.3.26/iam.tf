# #############################################
# ###IAM policy for search function lambda
# #############################################

data "aws_iam_policy_document" "search_func_lambda_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"

  statement {
    sid = "cloudwatchlogspermissions"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:${local.region}:${local.aws_account_id}:*"]
  }
  statement {
    sid = "cloudwatchpermissions"
    actions = [
      "cloudwatch:PutMetricData"
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateNetworkInterface",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcs",
      "ec2:DescribeNetworkInterfaceAttribute",
      "ec2:DescribeNetworkInterfaces"
    ]
    resources = ["*"]
  }
  statement {
    sid = "OpenSearchPassRolePermissions"
    actions = [
      "sts:AssumeRole"
    ]
    resources = [aws_iam_role.opensearch_master_user_role.arn]
  }
  statement {
    sid = "InvokeLambdaPermisisons"
    actions = [
      "lambda:InvokeFunction"
    ]
    resources = [
      #"arn:aws:lambda:${local.region}:${local.aws_account_id}:function:udx-cust-mesh-d-queue-unresolved-entities-chandan", # for DEV only
      module.queue_unresolved_entities_func.lambda_function_arn
    ]
  }
  statement {
    sid = "DynamoDbpermissions"
    actions = [
      "es:ESHttpGet",
      "es:ESHttpHead"

    ]
    resources = [
      aws_opensearch_domain.opensearch.arn
    ]
  }


}


resource "aws_iam_policy" "search_func_lambda_policy" {
  name   = "${var.name_prefix}-search-func-lambda-policy"
  policy = data.aws_iam_policy_document.search_func_lambda_policy.json
}

# #############################################
# ###IAM policy for file status lambda
# #############################################

data "aws_iam_policy_document" "file_status_lambda_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "gets3access"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:ListBucket",
    ]
    resources = [
      data.aws_s3_bucket.idres_data_in.arn,
      "${data.aws_s3_bucket.idres_data_in.arn}/*",
      data.aws_s3_bucket.glue_code_bucket.arn,
      "${data.aws_s3_bucket.glue_code_bucket.arn}/*"
    ]
  }
  statement {
    sid = "Lists3access"
    actions = [
      "s3:ListBucket",
    ]
    resources = [
      "*"
    ]
  }
  statement {
    sid = "kmsPermissions"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:GenerateDataKey",
      "kms:GetKeyPolicy",
      "kms:DescribeKey"
    ]
    resources = [
      data.aws_kms_key.neptune_key_arn.arn,
      data.aws_kms_key.ddb_key_arn.arn,
    ]
  }


  statement {
    sid = "puts3access"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:PutObject",
      "s3:ListBucket",
      "s3:DeleteObject"
    ]
    resources = [
      data.aws_s3_bucket.idres_temp_bucket.arn,
      "${data.aws_s3_bucket.idres_temp_bucket.arn}/*",
    ]

  }
  statement {
    sid = "cloudwatchpermissions"
    actions = [
      "cloudwatch:PutMetricData"
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateNetworkInterface",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcs",
      "ec2:DescribeNetworkInterfaceAttribute",
      "ec2:DescribeNetworkInterfaces"
    ]
    resources = ["*"]
  }
  statement {
    sid = "gluePermisisons"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRuns",
      "glue:GetJobRun",
      "glue:GetJob"
    ]
    resources = [aws_glue_job.incr_id_res_job.arn,
      aws_glue_job.large_source_etl_job.arn,
      #"arn:aws:glue:${local.region}:${local.aws_account_id}:job/udx-cust-mesh-d-glue-large-src-job-copy" # for DEV only
    ]
  }
  statement {
    sid = "DynamoDbpermissions"
    actions = [
      "dynamodb:BatchGetItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:ListGlobalTables",
      "dynamodb:ConditionCheckItem",
      "dynamodb:PutItem",
      "dynamodb:DescribeTable",
      "dynamodb:DeleteItem",
      "dynamodb:GetItem",
      "dynamodb:UpdateItem",
      "dynamodb:Query",
      "dynamodb:PutItem"

    ]
    resources = [
      "${aws_dynamodb_table.file_status_table.arn}/*",
      "${aws_dynamodb_table.file_status_table.arn}",
      "${aws_dynamodb_table.glue_invoc_table.arn}/*",
      "${aws_dynamodb_table.glue_invoc_table.arn}",
      #"arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/udx-cust-mesh-d-file-status-tbl2",  # for DEV only
      #"arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/udx-cust-mesh-d-file-status-tbl",   # for DEV only
      #"arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/udx-cust-mesh-d-file-status-tbl/*", # for DEV only

      #"arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/udx-cust-mesh-d-idres-files-status-table-dev/index/*" #for Dev 
    ]
  }

  statement {
    sid = "cloudwatchlogspermissions"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:${local.region}:${local.aws_account_id}:*"]
  }
}


resource "aws_iam_policy" "file_status_lambda_policy" {
  name   = "${var.name_prefix}-filestatus-lambda-policy"
  policy = data.aws_iam_policy_document.file_status_lambda_policy.json
}

# #########################################
# ###IAM policy for small source etl lambda
# #########################################

data "aws_iam_policy_document" "small_source_etl_lambda_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "gets3access"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl"
    ]
    resources = [
      data.aws_s3_bucket.idres_data_in.arn,
      "${data.aws_s3_bucket.idres_data_in.arn}/*",
    ]
  }

  statement {
    sid = "Lists3access"
    actions = [
      "s3:ListBucket",
    ]
    resources = [
      "*"
    ]
  }
  statement {
    sid = "kmsPermissions"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:GenerateDataKey",
      "kms:GetKeyPolicy",
      "kms:DescribeKey"
    ]
    resources = [
      data.aws_kms_key.neptune_key_arn.arn,
      data.aws_kms_key.ddb_key_arn.arn,
    ]
  }

  statement {
    sid = "puts3access"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:PutObject",
      "s3:ListBucket",
      "s3:DeleteObject"
    ]
    resources = [
      data.aws_s3_bucket.idres_temp_bucket.arn,
      "${data.aws_s3_bucket.idres_temp_bucket.arn}/*",
      data.aws_s3_bucket.glue_code_bucket.arn,
      "${data.aws_s3_bucket.glue_code_bucket.arn}/*",
      data.aws_s3_bucket.glue_code_bucket.arn,
      "${data.aws_s3_bucket.glue_code_bucket.arn}/*",
      data.aws_s3_bucket.idres_data_out.arn,
      "${data.aws_s3_bucket.idres_data_out.arn}/*"
    ]
  }
  statement {
    sid = "cloudwatchpermissions"
    actions = [
      "cloudwatch:PutMetricData"
    ]
    resources = ["*"]
  }

  /*
  statement {
    sid = "gluePermisisons"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRuns",
      "glue:GetJobRun",
      "glue:GetJob"
    ]
    resources = [aws_glue_job.incr_id_res_job.arn,
      aws_glue_job.large_source_etl_job.arn
    ]
  }
*/

  statement {
    sid = "DynamoDbpermissions"
    actions = [
      "dynamodb:BatchGetItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:ListGlobalTables",
      "dynamodb:ConditionCheckItem",
      "dynamodb:PutItem",
      "dynamodb:DescribeTable",
      "dynamodb:DeleteItem",
      "dynamodb:GetItem",
      "dynamodb:UpdateItem",
      "dynamodb:Query",
      "dynamodb:PutItem"

    ]
    resources = [
      "${aws_dynamodb_table.file_status_table.arn}/*",
      "${aws_dynamodb_table.file_status_table.arn}",
      #"arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/udx-cust-mesh-d-file-status-tbl2",  # for DEV only
      #"arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/udx-cust-mesh-d-file-status-tbl",   # for DEV only
      #"arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/udx-cust-mesh-d-file-status-tbl/*", # for DEV only
    ]
  }

  statement {
    actions = [
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateNetworkInterface",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcs",
      "ec2:DescribeNetworkInterfaceAttribute",
      "ec2:DescribeNetworkInterfaces"
    ]
    resources = ["*"]
  }
  statement {
    sid = "cloudwatchlogspermissions"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:${local.region}:${local.aws_account_id}:*"]
  }
}

resource "aws_iam_policy" "small_etl_lambda_policy" {
  name   = "${var.name_prefix}-small-etl-lambda-policy"
  policy = data.aws_iam_policy_document.small_source_etl_lambda_policy.json
}

# #########################################
# ###IAM role for incremental glue lambda
# #########################################

data "aws_iam_policy_document" "incr_glue_lambda_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "gets3access"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:ListBucket"
    ]
    resources = [
      data.aws_s3_bucket.idres_data_in.arn,
      "${data.aws_s3_bucket.idres_data_in.arn}/*",
      data.aws_s3_bucket.glue_code_bucket.arn,
      "${data.aws_s3_bucket.glue_code_bucket.arn}/*"
    ]
  }
  # statement {
  #   sid = "DynamoDbpermissions"
  #   actions = [
  #     "dynamodb:BatchGetItem",
  #     "dynamodb:BatchWriteItem",
  #     "dynamodb:ListGlobalTables",
  #     "dynamodb:ConditionCheckItem",
  #     "dynamodb:PutItem",
  #     "dynamodb:DescribeTable",
  #     "dynamodb:DeleteItem",
  #     "dynamodb:GetItem",
  #     "dynamodb:UpdateItem"
  #   ]
  #   resources = [aws_dynamodb_table.file_status_table.arn]
  # }

  statement {
    sid = "gluejobaccess"
    actions = [
      "glue:UpdateSourceControlFromJob",
      "glue:GetJobs",
      "glue:BatchStopJobRun",
      "glue:DeleteJob",
      "glue:ListJobs",
      "glue:CreateJob",
      "glue:ResetJobBookmark",
      "glue:BatchGetJobs",
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:UpdateJobFromSourceControl",
      "glue:UntagResource",
      "glue:UpdateJob",
      "glue:GetJobBookmark",
      "glue:TagResource",
      "glue:GetJobRuns",
      "glue:GetJob"
    ]
    resources = [
      "*"
    ]

  }
  statement {
    sid = "puts3access"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:PutObject",
      "s3:ListBucket",
      "s3:DeleteObject",
    ]
    resources = [
      data.aws_s3_bucket.idres_temp_bucket.arn,
      "${data.aws_s3_bucket.idres_temp_bucket.arn}/*",
    ]

  }
  statement {
    sid = "cloudwatchpermissions"
    actions = [
      "cloudwatch:PutMetricData"
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateNetworkInterface",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcs",
      "ec2:DescribeNetworkInterfaceAttribute",
      "ec2:DescribeNetworkInterfaces"
    ]
    resources = ["*"]
  }
  statement {
    sid = "cloudwatchlogspermissions"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:${local.region}:${local.aws_account_id}:*"]
  }
}

resource "aws_iam_policy" "incr_glue_lambda_policy" {
  name   = "${var.name_prefix}-incr-glue-lambda-policy"
  policy = data.aws_iam_policy_document.incr_glue_lambda_policy.json
}

# #########################################
# ###IAM role for main id res glue
# #########################################

resource "aws_iam_role" "glue_id_res_job_role" {
  name = "${var.name_prefix}-main-id-res-glue-job-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      },
    ]
  })
  permissions_boundary = var.permission_boundary_arn
}

data "aws_iam_policy_document" "main_id_res_glue_job_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "S3Permissions"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:PutObject",
      "s3:ListBucket",
      "s3:DeleteObject",
      "s3:GetBucketLocation"
    ]
    resources = [
      data.aws_s3_bucket.idres_temp_bucket.arn,
      "${data.aws_s3_bucket.idres_temp_bucket.arn}/*",
      data.aws_s3_bucket.idres_data_out.arn,
      "${data.aws_s3_bucket.idres_data_out.arn}/*",
      data.aws_s3_bucket.glue_code_bucket.arn,
      "${data.aws_s3_bucket.glue_code_bucket.arn}/*",
      data.aws_s3_bucket.historical_data_bucket.arn,
      "${data.aws_s3_bucket.historical_data_bucket.arn}/*",

      data.aws_s3_bucket.idres_scratch_space_bucket.arn,
      "${data.aws_s3_bucket.idres_scratch_space_bucket.arn}/*",
      data.aws_s3_bucket.idres_data_in.arn,
      "${data.aws_s3_bucket.idres_data_in.arn}/*"
    ]
  }

  statement {
    sid = "OpenSearchPassRolePermissions"
    actions = [
      "sts:AssumeRole"
    ]
    resources = [aws_iam_role.opensearch_master_user_role.arn,
      #"arn:aws:iam::${local.aws_account_id}:role/udx-cust-mesh-d-opensearch-iam-master-user-manual", # for DEV only   
    ]
  }
  statement {
    sid = "OpenSearchPermissions"
    actions = [
      "es:Describe*"
    ]
    resources = [aws_opensearch_domain.opensearch.arn]
  }

    statement {
    sid = "OpenSearchHttpPermissions"
    actions = [
      "es:ESHttpGet",
      "es:ESHttpHead",
      "es:ESHttpPost",
      "es:ESHttpPut",
      "es:ESHttpDelete"
    ]
    resources = [
      aws_opensearch_domain.opensearch.arn,
      "${aws_opensearch_domain.opensearch.arn}/*"
    ]
  }
  statement {
    sid = "KMSPermissions"
    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey",
      "kms:DescribeKey",
    ]
    resources = [
      data.aws_kms_key.ddb_key_arn.arn,
      data.aws_kms_key.neptune_key_arn.arn,
      data.aws_kms_key.opensearch_key_arn.arn,
      data.aws_kms_key.sqs_sns_key_arn.arn
    ]
  }
  statement {
    sid = "cloudwatchpermissions"
    actions = [
      "cloudwatch:PutMetricData"
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateNetworkInterface",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcs",
      "ec2:DescribeNetworkInterfaceAttribute",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DescribeTags",
      "ec2:CreateTags",
      "acm:DescribeCertificate",
      "ec2:DescribeVpcEndpoints",
      "ec2:DescribeRouteTables"
    ]
    resources = ["*"]
  }

  statement {
    sid = "cloudwatchlogspermissions"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:${local.region}:${local.aws_account_id}:*"]
  }

  statement {
    sid = "entityresolutionJobPermissions"
    actions = [
      "entityresolution:*SchemaMapping*",
      "entityresolution:ListProviderServices",
      "entityresolution:*MatchingWorkflow*",
      "entityresolution:*MatchingJob*"
    ]
    resources = [
      "arn:aws:entityresolution:${local.region}:${local.aws_account_id}:*/*"
    ]
  }
  statement {
    sid = "RequiredByERService"
    actions = [
      "events:*Rule",
      "events:*Targets"
    ]
    resources = ["arn:aws:events:${local.region}:${local.aws_account_id}:rule/entity-resolution-automatic*"]
  }
  statement {
    actions = [
      "glue:*Session",
      "glue:*Table",
      "glue:*Connection",
      "glue:GetDatabases",
      "glue:*Partition",
      "glue:*JobRun",
      "glue:*JobRuns",
      "glue:*Statement",
      "glue:TagResource",
      "glue:*Database"
    ]
    resources = ["*"]
  }

  statement {
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults"
    ]
    resources = ["*"]
  }

  statement {
    sid = "passroleToER"
    actions = [
      "iam:PassRole"
    ]
    resources = [
      aws_iam_role.er_service_role.arn
    ]
  }

  statement {
    sid = "DynamoDbpermissions"
    actions = [
      "dynamodb:BatchGetItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:ListGlobalTables",
      "dynamodb:ConditionCheckItem",
      "dynamodb:PutItem",
      "dynamodb:DescribeTable",
      "dynamodb:DeleteItem",
      "dynamodb:GetItem",
      "dynamodb:UpdateItem",
      "dynamodb:Query",
      "dynamodb:Scan"
    ]
    resources = [
      "arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/${var.name_prefix}*"
    ]
  }

  dynamic "statement" {
    for_each = trimspace(var.snowflake_jdbc_secret_arn) != "" ? [1] : []
    content {
      sid = "SecretsManagerSnowflakeIncrJob"
      actions = [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret"
      ]
      resources = [var.snowflake_jdbc_secret_arn]
    }
  }

  statement {
    sid = "neptunePermisisons"
    actions = [
      "neptune-db:*"
    ]
    resources = [
      aws_neptune_cluster.neptune_cluster.arn,
      "arn:aws:neptune-db:${local.region}:${local.aws_account_id}:${aws_neptune_cluster.neptune_cluster.cluster_resource_id}/*"
    ]
  }
}
resource "aws_iam_role_policy" "main_id_res_glue_job_policy" {
  name   = "${var.name_prefix}-main-id-res-glue-job-policy"
  role   = aws_iam_role.glue_id_res_job_role.id
  policy = data.aws_iam_policy_document.main_id_res_glue_job_policy.json
}

#########################################
###IAM role for historical large source
#########################################

resource "aws_iam_role" "large_source_etl_job_role" {
  name = "${var.name_prefix}-large-source-etl-job-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      },
    ]
  })
  permissions_boundary = var.permission_boundary_arn
}

data "aws_iam_policy_document" "large_source_etl_job_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "S3Permissions"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:PutObject",
      "s3:ListBucket",
      "s3:DeleteObject"
    ]
    resources = [
      data.aws_s3_bucket.idres_temp_bucket.arn,
      "${data.aws_s3_bucket.idres_temp_bucket.arn}/*",
      data.aws_s3_bucket.idres_data_out.arn,
      "${data.aws_s3_bucket.idres_data_out.arn}/*",
    ]
  }
  statement {
    sid = "DynamoDbPermissions"
    actions = [
      "dynamodb:BatchGetItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:UpdateTimeToLive",
      "dynamodb:ConditionCheckItem",
      "dynamodb:PutItem",
      "dynamodb:DeleteItem",
      "dynamodb:Scan",
      "dynamodb:ListTagsOfResource",
      "dynamodb:Query",
      "dynamodb:UpdateItem",
      "dynamodb:DescribeTimeToLive",
      "dynamodb:DeleteTable",
      "dynamodb:PartiQLSelect",
      "dynamodb:DescribeTable",
      "dynamodb:GetItem",
      "dynamodb:GetResourcePolicy",
      "dynamodb:ImportTable",
      "dynamodb:UpdateTable"
    ]
    resources = [
      #"arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/udx-cust-mesh-d-file-status-tbl2", # for DEV only
      #"arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/udx-cust-mesh-d-file-status-tbl",  # for DEV only
      "${aws_dynamodb_table.file_status_table.arn}",
      "${aws_dynamodb_table.blocklist_table_name.arn}",
      "${aws_dynamodb_table.udx_address_store_table.arn}"
    ]
  }
  statement {
    sid = "DynamoDbListPermissions"
    actions = [
      "dynamodb:ListGlobalTables",
      "dynamodb:ListTables",
      "dynamodb:DescribeLimits"
    ]
    resources = [
      "*"
    ]
  }
  statement {
    sid = "KMSPermissions"
    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GetKeyPolicy",
      "kms:DescribeKey",
    ]
    resources = [
      data.aws_kms_key.ddb_key_arn.arn
    ]
  }

  statement {
    sid = "cloudwatchpermissions"
    actions = [
      "cloudwatch:DescribeInsightRules",
      "cloudwatch:PutDashboard",
      "cloudwatch:PutMetricData",
      "cloudwatch:GenerateQuery",
      "cloudwatch:GetDashboard",
      "cloudwatch:GetMetricData",
      "cloudwatch:EnableInsightRules",
      "logs:StopQuery",
      "cloudwatch:GetMetricStatistics",
      "cloudwatch:CreateServiceLevelObjective",
      "cloudwatch:ListMetrics",
      "logs:GetLogDelivery",
      "cloudwatch:GetServiceData",
      "cloudwatch:PutMetricStream",
      "cloudwatch:PutInsightRule",
      "cloudwatch:PutMetricAlarm",
      "cloudwatch:DeleteDashboards",
      "cloudwatch:EnableAlarmActions",
      "cloudwatch:ListDashboards",
      "cloudwatch:GetMetricStream"

    ]
    resources = ["*"]
  }
  statement {
    sid = "cloudwatchlogspermissions"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:PutSubscriptionFilter",
      "logs:GetLogGroupFields",
      "logs:PutDeliveryDestination",
      "logs:GetLogEvents",
      "logs:PutSubscriptionFilter",
      "logs:PutLogEvents",
      "logs:CreateLogStream",
      "logs:CreateLogGroup"
    ]
    resources = [
      "arn:aws:logs:${local.region}:${local.aws_account_id}:log-group:*",
      "arn:aws:logs:${local.region}:${local.aws_account_id}:log-group:*:log-stream:*",
      "arn:aws:logs:${local.region}:${local.aws_account_id}:delivery-destination:*",
      "arn:aws:logs:${local.region}:${local.aws_account_id}:destination:*"
    ]
  }
  statement {
    actions = [
      "entityresolution:GetPolicy",
      "entityresolution:ListIdMappingWorkflows",
      "entityresolution:ListSchemaMappings",
      "entityresolution:ListMatchingWorkflows",
      "entityresolution:ListTagsForResource",
      "entityresolution:ListIdNamespaces"
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "entityresolution:GetPolicy",
      "entityresolution:ListIdMappingWorkflows",
      "entityresolution:ListSchemaMappings",
      "entityresolution:ListMatchingWorkflows",
      "entityresolution:ListTagsForResource",
      "entityresolution:ListIdNamespaces"
    ]
    resources = ["*"]
  }

  statement {
    actions = [
      "glue:CancelStatement",
      "glue:GetCrawlers",
      "glue:GetJobs",
      "glue:GetTriggers",
      "glue:StartCompletion",
      "glue:ListTriggers",
      "glue:ListCrawls",
      "glue:ListJobs",
      "glue:CreateCrawler",
      "glue:GetMapping",
      "glue:CreateScript",
      "glue:ResetJobBookmark",
      "glue:GetStatement",
      "glue:UntagResource",
      "glue:ListCrawlers",
      "glue:GetCrawlerMetrics",
      "glue:ListWorkflows",
      "glue:TagResource"
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "glue:GetStatement"
    ]
    resources = ["arn:aws:glue:${local.region}:${local.aws_account_id}:session/*"]
  }
  statement {
    actions = [
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateNetworkInterface",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcs",
      "ec2:DescribeNetworkInterfaceAttribute",
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateTags",
      "acm:DescribeCertificate",
      "ec2:DescribeVpcEndpoints",
      "ec2:DescribeRouteTables"

    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "glue:GetStatement"
    ]
    resources = ["arn:aws:glue:${local.region}:${local.aws_account_id}:session/*"]
  }
  statement {
    actions = [
      "glue:BatchCreatePartition",
      "glue:CreatePartitionIndex",
      "glue:GetCrawler",
      "glue:GetPartitions",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:DeleteWorkflow",
      "glue:UpdateCrawler",
      "glue:GetSchema",
      "glue:DeleteSchema",
      "glue:UpdateWorkflow",
      "glue:UpdateTrigger",
      "glue:GetTrigger",
      "glue:RunStatement",
      "glue:GetJobRun",
      "glue:StartWorkflowRun",
      "glue:UpdateDatabase",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:DeleteCrawler",
      "glue:GetWorkflowRun",
      "glue:UpdateSchema",
      "glue:CreateConnection",
      "glue:GetPartitionIndexes",
      "glue:GetPartition",
      "glue:DeleteConnection",
      "glue:BatchGetWorkflows",
      "glue:BatchGetJobs",
      "glue:CreateSchema",
      "glue:StartJobRun",
      "glue:BatchDeleteTable",
      "glue:CreateWorkflow",
      "glue:DeletePartition",
      "glue:GetJob",
      "glue:GetWorkflow",
      "glue:ListSchemaVersions",
      "glue:NotifyEvent",
      "glue:GetConnections",
      "glue:DeleteDatabase",
      "glue:PassConnection",
      "glue:DeleteTableVersion",
      "glue:CreateTrigger",
      "glue:BatchDeletePartition",
      "glue:StopTrigger",
      "glue:StopCrawler",
      "glue:DeleteJob",
      "glue:StartTrigger",
      "glue:CreateJob",
      "glue:GetConnection",
      "glue:StartCrawler",
      "glue:ListSchemas",
      "glue:DeleteSession",
      "glue:UpdateJob",
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:ResumeWorkflowRun",
      "glue:BatchGetPartition",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetDatabase",
      "glue:PutSchemaVersionMetadata",
      "glue:UpdateConnection",
      "glue:BatchGetCrawlers",
      "glue:GetSession",
      "glue:BatchGetTriggers",
      "glue:CreateDatabase",
      "glue:CreateSession",
      "glue:StopSession",
      "glue:GetWorkflowRuns",
      "glue:DeleteTrigger",
      "glue:DeleteSchemaVersions",
      "glue:GetJobRuns"
    ]
    resources = [
      "arn:aws:glue:${local.region}:${local.aws_account_id}:schema/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:registry/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:session/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:connection/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:catalog",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:userDefinedFunction/*/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:job/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:workflow/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:table/*/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:database/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:trigger/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:crawler/*"
    ]
  }
  statement {
    actions = [
      "iam:PassRole"
    ]
    resources = [aws_iam_role.er_service_role.arn
    ]
  }
  statement {
    actions = [
      "s3:GetObjectRetention",
      "s3:DeleteObjectVersion",
      "s3:ListBucketVersions",
      "s3:GetObjectAttributes",
      "s3:RestoreObject",
      "s3:ListBucket",
      "s3:GetObjectLegalHold",
      "s3:PutObject",
      "s3:GetObjectAcl",
      "s3:GetObject",
      "s3:GetObjectTagging",
      "s3:DeleteObject",
      "s3:GetBucketLocation"
    ]
    resources = ["arn:aws:s3:::${var.name_prefix}*"]
  }
  statement {
    actions = [
      "s3:ListAllMyBuckets",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "large_source_etl_job_policy" {
  name   = "${var.name_prefix}-large-source-etl-job-policy"
  role   = aws_iam_role.large_source_etl_job_role.id
  policy = data.aws_iam_policy_document.large_source_etl_job_policy.json
}

#########################################
###IAM role for Neptune data source
#########################################

#create IAM role for Neptune data source
resource "aws_iam_role" "neptune_role" {
  name = "${var.name_prefix}-neptune-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "rds.amazonaws.com"
        }
      },
    ]
  })
  permissions_boundary = var.permission_boundary_arn

}

data "aws_iam_policy_document" "neptune_s3_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "S3Permissions"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:PutObject",
      "s3:ListBucket",
      "s3:DeleteObject"
    ]
    resources = [
      data.aws_s3_bucket.idres_temp_bucket.arn,
      "${data.aws_s3_bucket.idres_temp_bucket.arn}/*",
      data.aws_s3_bucket.idres_data_out.arn,
      "${data.aws_s3_bucket.idres_data_out.arn}/*",
      data.aws_s3_bucket.idres_data_in.arn,
      "${data.aws_s3_bucket.idres_data_in.arn}/*",
      data.aws_s3_bucket.historical_data_bucket.arn,
      "${data.aws_s3_bucket.historical_data_bucket.arn}/*"


    ]
  }
  statement {
    sid = "cloudwatchpermissions"
    actions = [
      "cloudwatch:PutMetricData"
    ]
    resources = ["*"]
  }
  statement {
    sid = "cloudwatchlogspermissions"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:${local.region}:${local.aws_account_id}:*"]
  }
}
resource "aws_iam_role_policy" "neptune_policy" {
  name   = "${var.name_prefix}-neptune-policy"
  role   = aws_iam_role.neptune_role.id
  policy = data.aws_iam_policy_document.neptune_s3_policy.json
}



#########################################
###IAM role for Opensearch cluster
#########################################

#create IAM role for Neptune data source
resource "aws_iam_role" "idres_opensearch_role" {
  name = "${var.name_prefix}-opensearch-role2-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "opensearchservice.amazonaws.com"
        }
      },
    ]
  })
  permissions_boundary = var.permission_boundary_arn
  #managed_policy_arns = ["arn:aws:iam::aws:policy/aws-service-role/AmazonOpenSearchServiceRolePolicy"]

}

data "aws_iam_policy_document" "opensearch_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"

  statement {
    sid = "ec2permissions"
    actions = [
      "ec2:CreateNetworkInterface"
    ]
    resources = [
      "arn:aws:ec2:*:*:network-interface/*",
      "arn:aws:ec2:*:*:subnet/*",
      "arn:aws:ec2:*:*:security-group/*"
    ]
  }
  statement {
    sid = "ec2permissions2"
    actions = [
      "ec2:DescribeNetworkInterfaces"
    ]
    resources = [
      "*"
    ]
  }
  statement {
    sid = "Stmt1480452973144"
    actions = [
      "ec2:DeleteNetworkInterface"
    ]
    resources = [
      "arn:aws:ec2:*:*:network-interface/*"
    ]
  }

  statement {
    sid = "Stmt1480452973149"
    actions = [
      "ec2:AssignIpv6Addresses"
    ]
    resources = [
      "arn:aws:ec2:*:*:network-interface/*"
    ]
  }
  statement {
    sid = "Stmt1480452973165"
    actions = [
      "ec2:ModifyNetworkInterfaceAttribute"
    ]
    resources = [
      "arn:aws:ec2:*:*:network-interface/*",
      "arn:aws:ec2:*:*:security-group/*"
    ]
  }
  statement {
    sid = "Stmt1480452973150"
    actions = [
      "ec2:UnAssignIpv6Addresses"
    ]
    resources = [
      "arn:aws:ec2:*:*:network-interface/*"
    ]
  }
  statement {
    sid = "Stmt1480452973154"
    actions = [
      "ec2:DescribeSecurityGroups"
    ]
    resources = [
      "*"
    ]
  }
  statement {
    sid = "Stmt1480452973164"
    actions = [
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcs",
      "ec2:DescribeTags",
      "acm:DescribeCertificate",
      "ec2:DescribeVpcEndpoints"
    ]
    resources = [
      "*"
    ]
  }
  statement {
    sid = "Stmt1480452973184"
    actions = [
      "elasticloadbalancing:AddListenerCertificates",
      "elasticloadbalancing:RemoveListenerCertificates"
    ]
    resources = [
      "arn:aws:elasticloadbalancing:*:*:listener/*"
    ]
  }
  statement {
    sid = "Stmt1480452973194"
    actions = [
      "ec2:CreateTags"
    ]
    resources = [
      "arn:aws:ec2:*:*:network-interface/*"
    ]
  }
  statement {
    sid = "cloudwatchlogs"
    actions = [
      "cloudwatch:PutMetricData",
    ]
    resources = [
      "*"
    ]
  }
  statement {
    sid = "Stmt1480452973197"
    actions = [
      "ec2:CreateVpcEndpoint",
      "ec2:ModifyVpcEndpoint"
    ]
    resources = [
      "arn:aws:ec2:*:*:vpc/*",
      "arn:aws:ec2:*:*:security-group/*",
      "arn:aws:ec2:*:*:subnet/*",
      "arn:aws:ec2:*:*:route-table/*"
    ]
  }
  statement {
    sid = "Stmt1480452973199"
    actions = [
      "ec2:CreateVpcEndpoint",
      "ec2:ModifyVpcEndpoint",
      "ec2:DeleteVpcEndpoints"
    ]
    resources = [
      "arn:aws:ec2:*:*:vpc/*",
      "arn:aws:ec2:*:*:security-group/*",
      "arn:aws:ec2:*:*:subnet/*",
      "arn:aws:ec2:*:*:route-table/*"
    ]
  }

}
resource "aws_iam_role_policy" "opensearch_policy" {

  name   = "${var.name_prefix}-opensearch-policy"
  role   = aws_iam_role.neptune_role.id
  policy = data.aws_iam_policy_document.opensearch_policy.json
}

#########################################
###IAM role for Sagemaker Notebook
#########################################

resource "aws_iam_role" "sagemaker_notebook_role" {
  name = "${var.name_prefix}-sagemaker-notebook-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "sagemaker.amazonaws.com"
        }
      },
    ]
  })
  permissions_boundary = var.permission_boundary_arn

}

data "aws_iam_policy_document" "sagemaker_notebook_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "S3Permissions"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:PutObject",
      "s3:ListBucket",
      "s3:DeleteObject"
    ]
    resources = [
      data.aws_s3_bucket.idres_temp_bucket.arn,
      "${data.aws_s3_bucket.idres_temp_bucket.arn}/*",
      data.aws_s3_bucket.idres_data_out.arn,
      "${data.aws_s3_bucket.idres_data_out.arn}/*",
      data.aws_s3_bucket.idres_data_in.arn,
      "${data.aws_s3_bucket.idres_data_in.arn}/*",
      "arn:aws:s3:::aws-neptune-notebook",
      "arn:aws:s3:::aws-neptune-notebook/*",
      data.aws_s3_bucket.historical_data_bucket.arn,
      "${data.aws_s3_bucket.historical_data_bucket.arn}/*",
      data.aws_s3_bucket.glue_code_bucket.arn,
      "${data.aws_s3_bucket.glue_code_bucket.arn}/*"
    ]
  }
  statement {
    sid = "OpenSearchPermissions"
    actions = [
      "es:Describe*"
    ]
    resources = [aws_opensearch_domain.opensearch.arn]
  }
  statement {
    sid = "KMSPermissions"
    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GetKeyPolicy",
      "kms:DescribeKey",
    ]
    resources = [
      data.aws_kms_key.ddb_key_arn.arn
    ]
  }
  statement {
    sid = "cloudwatchpermissions"
    actions = [
      "cloudwatch:PutMetricData"
    ]
    resources = ["*"]
  }
  statement {
    sid = "SageMakerNotebookAccess"
    actions = [
      "sagemaker:DescribeNotebookInstance"
    ]
    resources = ["arn:aws:sagemaker:${local.region}:${local.aws_account_id}:notebook-instance/*"]
  }
  statement {
    sid = "neptunePermissions"
    actions = [
      "neptune-db:*"
    ]
    resources = [
      "arn:aws:neptune-db:${local.region}:${local.aws_account_id}:${aws_neptune_cluster.neptune_cluster.cluster_resource_id}/*"
    ]
  }
  statement {
    sid = "cloudwatchlogspermissions"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "cloudwatch:DescribeInsightRules",
      "cloudwatch:PutDashboard",
      "cloudwatch:PutMetricData",
      "cloudwatch:GenerateQuery",
      "cloudwatch:GetDashboard",
      "cloudwatch:GetMetricData",
      "cloudwatch:EnableInsightRules",
      "logs:StopQuery",
      "cloudwatch:GetMetricStatistics",
      "cloudwatch:CreateServiceLevelObjective",
      "cloudwatch:ListMetrics",
      "logs:GetLogDelivery",
      "cloudwatch:GetServiceData",
      "cloudwatch:PutMetricStream",
      "cloudwatch:PutInsightRule",
      "cloudwatch:PutMetricAlarm",
      "cloudwatch:DeleteDashboards",
      "cloudwatch:EnableAlarmActions",
      "cloudwatch:ListDashboards",
      "cloudwatch:GetMetricStream"
    ]
    resources = ["arn:aws:logs:${local.region}:${local.aws_account_id}:log-group:/aws/sagemaker/*"]
  }
  statement {
    sid = "DynamoDbpermissions"
    actions = [
      "dynamodb:BatchGetItem",
      "dynamodb:ListGlobalTables",
      "dynamodb:ConditionCheckItem",
      "dynamodb:PutItem",
      "dynamodb:DescribeTable",
      "dynamodb:GetItem",
      "dynamodb:Query"
    ]
    resources = [
      "arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/${var.name_prefix}*"
    ]
  }
}
resource "aws_iam_policy" "sagemaker_notebook_policy" {
  name   = "${var.name_prefix}-sagemaker-notebook-policy"
  policy = data.aws_iam_policy_document.sagemaker_notebook_policy.json
}

resource "aws_iam_policy_attachment" "sagemaker_notebook_policy_attachment" {
  name       = "${var.name_prefix}-read-only-sagemaker-notebook-policy-attachment"
  roles      = [aws_iam_role.sagemaker_notebook_role.name]
  policy_arn = aws_iam_policy.sagemaker_notebook_policy.arn
}

#########################################
###IAM role for read only Sagemaker Notebook
#########################################

resource "aws_iam_role" "read_only_sagemaker_notebook_role" {
  name = "${var.name_prefix}-read-only-sagemaker-notebook-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "sagemaker.amazonaws.com"
        }
      },
    ]
  })
  permissions_boundary = var.permission_boundary_arn

}

data "aws_iam_policy_document" "read_only_sagemaker_notebook_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "S3Permissions"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:ListBucket"
    ]
    resources = [
      data.aws_s3_bucket.idres_temp_bucket.arn,
      "${data.aws_s3_bucket.idres_temp_bucket.arn}/*",
      data.aws_s3_bucket.idres_data_out.arn,
      "${data.aws_s3_bucket.idres_data_out.arn}/*",
      data.aws_s3_bucket.idres_data_in.arn,
      "${data.aws_s3_bucket.idres_data_in.arn}/*",
      "arn:aws:s3:::aws-neptune-notebook",
      "arn:aws:s3:::aws-neptune-notebook/*",
      data.aws_s3_bucket.historical_data_bucket.arn,
      "${data.aws_s3_bucket.historical_data_bucket.arn}/*",
      data.aws_s3_bucket.glue_code_bucket.arn,
      "${data.aws_s3_bucket.glue_code_bucket.arn}/*"
    ]
  }
  statement {
    sid = "OpenSearchPermissions"
    actions = [
      "es:Describe*"
    ]
    resources = [aws_opensearch_domain.opensearch.arn]
  }
  statement {
    sid = "KMSPermissions"
    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GetKeyPolicy",
      "kms:DescribeKey",
    ]
    resources = [
      data.aws_kms_key.ddb_key_arn.arn
    ]
  }
  statement {
    sid = "cloudwatchpermissions"
    actions = [
      "cloudwatch:PutMetricData"
    ]
    resources = ["*"]
  }
  statement {
    sid = "SageMakerNotebookAccess"
    actions = [
      "sagemaker:DescribeNotebookInstance"
    ]
    resources = ["arn:aws:sagemaker:${local.region}:${local.aws_account_id}:notebook-instance/*"]
  }
  statement {
    sid = "neptunePermissions"
    actions = [
      "neptune-db:List*",
      "neptune-db:Get*",
      "neptune-db:ReadDataViaQuery"
    ]
    resources = [
      "arn:aws:neptune-db:${local.region}:${local.aws_account_id}:${aws_neptune_cluster.neptune_cluster.cluster_resource_id}/*"
    ]
  }
  statement {
    sid = "cloudwatchlogspermissions"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "cloudwatch:DescribeInsightRules",
      "cloudwatch:PutDashboard",
      "cloudwatch:PutMetricData",
      "cloudwatch:GenerateQuery",
      "cloudwatch:GetDashboard",
      "cloudwatch:GetMetricData",
      "cloudwatch:EnableInsightRules",
      "logs:StopQuery",
      "cloudwatch:GetMetricStatistics",
      "cloudwatch:CreateServiceLevelObjective",
      "cloudwatch:ListMetrics",
      "logs:GetLogDelivery",
      "cloudwatch:GetServiceData",
      "cloudwatch:PutMetricStream",
      "cloudwatch:PutInsightRule",
      "cloudwatch:PutMetricAlarm",
      "cloudwatch:DeleteDashboards",
      "cloudwatch:EnableAlarmActions",
      "cloudwatch:ListDashboards",
      "cloudwatch:GetMetricStream"
    ]
    resources = ["arn:aws:logs:${local.region}:${local.aws_account_id}:log-group:/aws/sagemaker/*"]
  }
  statement {
    sid = "DynamoDbpermissions"
    actions = [
      "dynamodb:BatchGetItem",
      "dynamodb:ListGlobalTables",
      "dynamodb:ConditionCheckItem",
      "dynamodb:DescribeTable",
      "dynamodb:GetItem",
      "dynamodb:Query"
    ]
    resources = [
      "arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/${var.name_prefix}*"
    ]
  }
}
resource "aws_iam_policy" "read_only_sagemaker_notebook_policy" {
  name   = "${var.name_prefix}-read-only-sagemaker-notebook-policy"
  policy = data.aws_iam_policy_document.read_only_sagemaker_notebook_policy.json

}
resource "aws_iam_policy_attachment" "read_only_sagemaker_notebook_policy_attachment" {
  name       = "${var.name_prefix}-read-only-sagemaker-notebook-policy-attachment"
  roles      = [aws_iam_role.read_only_sagemaker_notebook_role.name]
  policy_arn = aws_iam_policy.read_only_sagemaker_notebook_policy.arn
}

#########################################
###IAM role for Opensearch Master User
#########################################

resource "aws_iam_role" "opensearch_master_user_role" {
  name = "${var.name_prefix}-opensearch-master-user-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${local.aws_account_id}:root"
        }
        "Condition" : {
          "ArnLike" : {
            "aws:PrincipalArn" : "arn:aws:iam::${local.aws_account_id}:role/${var.name_prefix}*"
          }
        }
      },
    ]
  })
  permissions_boundary = var.permission_boundary_arn
}

data "aws_iam_policy_document" "master_snapshot_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "S3Permissions"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:PutObject",
      "s3:ListBucket",
      "s3:DeleteObject"
    ]
    resources = [
      data.aws_s3_bucket.idres_temp_bucket.arn,
      "${data.aws_s3_bucket.idres_temp_bucket.arn}/*",
      data.aws_s3_bucket.idres_scratch_space_bucket.arn,
      "${data.aws_s3_bucket.idres_scratch_space_bucket.arn}/*"
    ]
  }
  statement {
    sid = "passrolePermission"
    actions = [
      "iam:PassRole"
    ]
    resources = [
      aws_iam_role.opensearch_snapshot_role.arn
    ]
  }

}

resource "aws_iam_role_policy" "master_role_policy" {
  name   = "${var.name_prefix}-opensearch-master-policy"
  role   = aws_iam_role.opensearch_master_user_role.name
  policy = data.aws_iam_policy_document.master_snapshot_policy.json
}


#########################################
###IAM role for Glue Notebook
#########################################

resource "aws_iam_role" "glue_notebook_role" {
  name = "${var.name_prefix}-glue-notebook-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      },
    ]
  })
  permissions_boundary = var.permission_boundary_arn
}
data "aws_iam_policy_document" "glue_notebook_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "S3Permissions"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:PutObject",
      "s3:ListBucket",
      "s3:DeleteObject"
    ]
    resources = [
      data.aws_s3_bucket.idres_temp_bucket.arn,
      "${data.aws_s3_bucket.idres_temp_bucket.arn}/*",
      data.aws_s3_bucket.idres_data_out.arn,
      "${data.aws_s3_bucket.idres_data_out.arn}/*",
      data.aws_s3_bucket.historical_data_bucket.arn,
      "${data.aws_s3_bucket.historical_data_bucket.arn}/*"
    ]
  }
  statement {
    sid = "passrolePermissions"
    actions = [
      "iam:PassRole"
    ]
    resources = [
      aws_iam_role.large_source_etl_job_role.arn,
      aws_iam_role.glue_notebook_role.arn,
      aws_iam_role.opensearch_snapshot_role.arn
    ]
  }
  statement {
    sid = "assumerolePermissions"
    actions = [
      "sts:AssumeRole"
    ]
    resources = [
      aws_iam_role.opensearch_master_user_role.arn
    ]
  }


  statement {
    sid = "DynamoDbPermissions"
    actions = [
      "dynamodb:BatchGetItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:UpdateTimeToLive",
      "dynamodb:ConditionCheckItem",
      "dynamodb:PutItem",
      "dynamodb:DeleteItem",
      "dynamodb:Scan",
      "dynamodb:ListTagsOfResource",
      "dynamodb:Query",
      "dynamodb:UpdateItem",
      "dynamodb:DescribeTimeToLive",
      "dynamodb:DeleteTable",
      "dynamodb:PartiQLSelect",
      "dynamodb:DescribeTable",
      "dynamodb:GetItem",
      "dynamodb:GetResourcePolicy",
      "dynamodb:ImportTable",
      "dynamodb:UpdateTable"
    ]
    resources = [
      #"arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/udx-cust-mesh-d-file-status-tbl2", # for DEV only
      #"arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/udx-cust-mesh-d-file-status-tbl",  # for DEV only
      aws_dynamodb_table.file_status_table.arn,
      aws_dynamodb_table.blocklist_table_name.arn,
      aws_dynamodb_table.udx_address_store_table.arn
    ]
  }
  statement {
    sid = "DynamoDbListPermissions"
    actions = [
      "dynamodb:ListGlobalTables",
      "dynamodb:ListTables",
      "dynamodb:DescribeLimits"
    ]
    resources = [
      "*"
    ]
  }
  statement {
    sid = "KMSPermissions"
    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GetKeyPolicy",
      "kms:DescribeKey",
    ]
    resources = [
      data.aws_kms_key.ddb_key_arn.arn
    ]
  }
  statement {
    sid = "cloudwatchpermissions"
    actions = [
      "cloudwatch:DescribeInsightRules",
      "cloudwatch:PutDashboard",
      "cloudwatch:PutMetricData",
      "cloudwatch:GenerateQuery",
      "cloudwatch:GetDashboard",
      "cloudwatch:GetMetricData",
      "cloudwatch:EnableInsightRules",
      "logs:StopQuery",
      "cloudwatch:GetMetricStatistics",
      "cloudwatch:CreateServiceLevelObjective",
      "cloudwatch:ListMetrics",
      "logs:GetLogDelivery",
      "cloudwatch:GetServiceData",
      "cloudwatch:PutMetricStream",
      "cloudwatch:PutInsightRule",
      "cloudwatch:PutMetricAlarm",
      "cloudwatch:DeleteDashboards",
      "cloudwatch:EnableAlarmActions",
      "cloudwatch:ListDashboards",
      "cloudwatch:GetMetricStream"

    ]
    resources = ["*"]
  }
  statement {
    sid = "cloudwatchlogspermissions"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:PutSubscriptionFilter",
      "logs:GetLogGroupFields",
      "logs:PutDeliveryDestination",
      "logs:GetLogEvents",
      "logs:PutSubscriptionFilter",
      "logs:PutLogEvents",
      "logs:CreateLogStream",
      "logs:CreateLogGroup"
    ]
    resources = [
      "arn:aws:logs:${local.region}:${local.aws_account_id}:log-group:*",
      "arn:aws:logs:${local.region}:${local.aws_account_id}:log-group:*:log-stream:*",
      "arn:aws:logs:${local.region}:${local.aws_account_id}:delivery-destination:*",
      "arn:aws:logs:${local.region}:${local.aws_account_id}:destination:*"
    ]
  }

}
data "aws_iam_policy_document" "glue_notebook_policy2" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "entityresolutionJobPermissions"
    actions = [
      "entityresolution:GetIdMappingWorkflow",
      "entityresolution:GetSchemaMapping",
      "entityresolution:ListIdMappingJobs",
      "entityresolution:GetIdNamespace",
      "entityresolution:GetMatchingJob",
      "entityresolution:StartMatchingJob",
      "entityresolution:GetMatchId",
      "entityresolution:StartIdMappingJob",
      "entityresolution:GetIdMappingJob",
      "entityresolution:ListMatchingJobs",
      "entityresolution:ListProviderServices",
      "entityresolution:GetMatchingWorkflow"
    ]
    resources = [
      "arn:aws:entityresolution:${local.region}:${local.aws_account_id}:schemamapping/*",
      "arn:aws:entityresolution:${local.region}:${local.aws_account_id}:idnamespace/*",
      "arn:aws:entityresolution:${local.region}:${local.aws_account_id}:matchingworkflow/*",
      "arn:aws:entityresolution:${local.region}:${local.aws_account_id}:providerservice/*/*",
      "arn:aws:entityresolution:${local.region}:${local.aws_account_id}:idmappingworkflow/*"
    ]
  }
  statement {
    actions = [
      "glue:CancelStatement",
      "glue:GetCrawlers",
      "glue:GetJobs",
      "glue:GetTriggers",
      "glue:UpdateJob",
      "glue:StartCompletion",
      "glue:ListTriggers",
      "glue:ListCrawls",
      "glue:ListJobs",
      "glue:CreateCrawler",
      "glue:GetMapping",
      "glue:CreateScript",
      "glue:ResetJobBookmark",
      "glue:GetStatement",
      "glue:UntagResource",
      "glue:ListCrawlers",
      "glue:GetCrawlerMetrics",
      "glue:ListWorkflows",
      "glue:TagResource"
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "glue:GetStatement"
    ]
    resources = ["arn:aws:glue:${local.region}:${local.aws_account_id}:session/*"]
  }
  statement {
    actions = [
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateNetworkInterface",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcs",
      "ec2:DescribeNetworkInterfaceAttribute",
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateTags",
      "acm:DescribeCertificate",
      "ec2:DescribeVpcEndpoints",
      "ec2:DescribeRouteTables"

    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "glue:GetStatement"
    ]
    resources = ["arn:aws:glue:${local.region}:${local.aws_account_id}:session/*"]
  }
  statement {
    actions = [
      "glue:BatchCreatePartition",
      "glue:CreatePartitionIndex",
      "glue:GetCrawler",
      "glue:GetPartitions",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:DeleteWorkflow",
      "glue:UpdateCrawler",
      "glue:GetSchema",
      "glue:DeleteSchema",
      "glue:UpdateWorkflow",
      "glue:UpdateTrigger",
      "glue:GetTrigger",
      "glue:RunStatement",
      "glue:GetJobRun",
      "glue:StartWorkflowRun",
      "glue:UpdateDatabase",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:DeleteCrawler",
      "glue:GetWorkflowRun",
      "glue:UpdateSchema",
      "glue:CreateConnection",
      "glue:GetPartitionIndexes",
      "glue:GetPartition",
      "glue:DeleteConnection",
      "glue:BatchGetWorkflows",
      "glue:BatchGetJobs",
      "glue:CreateSchema",
      "glue:StartJobRun",
      "glue:BatchDeleteTable",
      "glue:CreateWorkflow",
      "glue:DeletePartition",
      "glue:GetJob",
      "glue:GetWorkflow",
      "glue:ListSchemaVersions",
      "glue:NotifyEvent",
      "glue:GetConnections",
      "glue:DeleteDatabase",
      "glue:PassConnection",
      "glue:DeleteTableVersion",
      "glue:CreateTrigger",
      "glue:BatchDeletePartition",
      "glue:StopTrigger",
      "glue:StopCrawler",
      "glue:DeleteJob",
      "glue:StartTrigger",
      "glue:CreateJob",
      "glue:GetConnection",
      "glue:StartCrawler",
      "glue:ListSchemas",
      "glue:DeleteSession",
      "glue:UpdateJob",
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:ResumeWorkflowRun",
      "glue:BatchGetPartition",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetDatabase",
      "glue:PutSchemaVersionMetadata",
      "glue:UpdateConnection",
      "glue:BatchGetCrawlers",
      "glue:GetSession",
      "glue:BatchGetTriggers",
      "glue:CreateDatabase",
      "glue:CreateSession",
      "glue:StopSession",
      "glue:GetWorkflowRuns",
      "glue:DeleteTrigger",
      "glue:DeleteSchemaVersions",
      "glue:GetJobRuns"
    ]
    resources = [
      "arn:aws:glue:${local.region}:${local.aws_account_id}:schema/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:registry/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:session/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:connection/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:catalog",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:userDefinedFunction/*/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:job/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:workflow/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:table/*/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:database/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:trigger/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:crawler/*"
    ]
  }

  statement {
    actions = [
      "s3:GetObjectRetention",
      "s3:DeleteObjectVersion",
      "s3:ListBucketVersions",
      "s3:GetObjectAttributes",
      "s3:RestoreObject",
      "s3:ListBucket",
      "s3:GetObjectLegalHold",
      "s3:PutObject",
      "s3:GetObjectAcl",
      "s3:GetObject",
      "s3:GetObjectTagging",
      "s3:DeleteObject",
      "s3:GetBucketLocation"
    ]
    resources = ["arn:aws:s3:::${var.name_prefix}*"
    ]
  }
  statement {
    actions = [
      "s3:ListAllMyBuckets",
    ]
    resources = ["*"]
  }
  statement {
    sid = "DynamoDbPermissions2"
    actions = [
      "dynamodb:BatchGetItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:UpdateTimeToLive",
      "dynamodb:ConditionCheckItem",
      "dynamodb:PutItem",
      "dynamodb:DeleteItem",
      "dynamodb:Scan",
      "dynamodb:ListTagsOfResource",
      "dynamodb:Query",
      "dynamodb:UpdateItem",
      "dynamodb:DescribeTimeToLive",
      "dynamodb:DeleteTable",
      "dynamodb:PartiQLSelect",
      "dynamodb:DescribeTable",
      "dynamodb:GetItem",
      "dynamodb:GetResourcePolicy",
      "dynamodb:ImportTable",
      "dynamodb:UpdateTable"
    ]
    resources = [
      #"arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/udx-cust-mesh-d-file-status-tbl2", # for DEV only
      #"arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/udx-cust-mesh-d-file-status-tbl",  # for DEV only
      "arn:aws:dynamodb:${local.region}:${local.aws_account_id}:table/${var.name_prefix}*"
    ]
  }
  statement {
    actions = [
      "neptune-db:Get*",
      "neptune-db:Read*",
      "neptune-db:ListLoaderJobs",
      "neptune-db:ResetDatabase"

    ]
    resources = [
      aws_neptune_cluster.neptune_cluster.arn,
      "arn:aws:neptune-db:${local.region}:${local.aws_account_id}:${aws_neptune_cluster.neptune_cluster.cluster_resource_id}/*"
    ]
  }
}

resource "aws_iam_role_policy" "glue_notebook_policy" {
  name   = "${var.name_prefix}-glue-notebook-policy"
  role   = aws_iam_role.glue_notebook_role.name
  policy = data.aws_iam_policy_document.glue_notebook_policy.json
}

resource "aws_iam_role_policy" "glue_notebook_policy2" {
  name   = "${var.name_prefix}-glue-notebook-policy2"
  role   = aws_iam_role.glue_notebook_role.name
  policy = data.aws_iam_policy_document.glue_notebook_policy2.json
}


#########################################
###ER SERvice Role
#########################################

resource "aws_iam_role" "er_service_role" {
  name = "${var.name_prefix}-er-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "entityresolution.amazonaws.com"
        }
        "Condition" : {
          "StringEquals" : {
            "aws:SourceAccount" : "${local.aws_account_id}"
          },
          "ArnLike" : {
            "aws:SourceArn" : "arn:aws:entityresolution:${local.region}:${local.aws_account_id}:matchingworkflow/*"
          }
        }
      },
    ]
  })
  permissions_boundary = var.permission_boundary_arn
}

data "aws_iam_policy_document" "er_service_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "GlueTablePermissions"
    actions = [
      "glue:GetDatabase",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
      "glue:GetTable"
    ]
    resources = [
      "arn:aws:glue:${local.region}:${local.aws_account_id}:table/*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:database/udx*",
      "arn:aws:glue:${local.region}:${local.aws_account_id}:catalog"
    ]
  }

  statement {
    sid = "logsPermissions"
    actions = [
      "logs:GetDelivery",
      "logs:GetLogEvents",
      "logs:PutLogEvents",
      "logs:GetLogRecord",
      "logs:StartQuery",
      "logs:StopQuery",
      "logs:GetLogDelivery",
      "logs:FilterLogEvents",
      "logs:GetLogGroupFields"
    ]
    resources = [
      "*"
    ]
  }

  statement {
    sid = "s3Permissions"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      "arn:aws:s3:::udx-cust-mesh*/*",
      "arn:aws:s3:::udx-cust-mesh*"
    ]
    condition {
      test     = "StringEquals"
      variable = "s3:ResourceAccount"
      values   = ["${local.aws_account_id}"]
    }
  }
}
resource "aws_iam_role_policy" "er_policy" {
  name   = "${var.name_prefix}-er-policy"
  role   = aws_iam_role.er_service_role.name
  policy = data.aws_iam_policy_document.er_service_policy.json
}


###########################################################
###IAM policy for search authorizer function lambda
###########################################################

data "aws_iam_policy_document" "search_authorizer_func_lambda_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"

  statement {
    sid = "cloudwatchlogspermissions"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:${local.region}:${local.aws_account_id}:*"]
  }
  statement {
    sid = "cloudwatchpermissions"
    actions = [
      "cloudwatch:PutMetricData"
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateNetworkInterface",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcs",
      "ec2:DescribeNetworkInterfaceAttribute",
      "ec2:DescribeNetworkInterfaces"
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "cognito-idp:AdminListGroupsForUser"
    ]
    resources = [aws_cognito_user_pool.search_api_user_pool.arn]
  }
}


resource "aws_iam_policy" "search_authorizer_func_lambda_policy" {
  name   = "${var.name_prefix}-search-authorizer-func-lambda-policy"
  policy = data.aws_iam_policy_document.search_authorizer_func_lambda_policy.json
}



###########################################################
###IAM policy for queue unresolved entities
###########################################################

data "aws_iam_policy_document" "queue_unresolved_entities_func_lambda_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "cloudwatchlogspermissions"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:${local.region}:${local.aws_account_id}:*"]
  }
  statement {
    sid = "cloudwatchpermissions"
    actions = [
      "cloudwatch:PutMetricData"
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateNetworkInterface",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcs",
      "ec2:DescribeNetworkInterfaceAttribute",
      "ec2:DescribeNetworkInterfaces"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "queue_unresolved_entities_func_lambda_policy" {
  name   = "${var.name_prefix}-queue-unresolved-entities-func-lambda-policy"
  policy = data.aws_iam_policy_document.queue_unresolved_entities_func_lambda_policy.json
}


###########################################################
###IAM policy for Incremental lambda starter 
###########################################################

data "aws_iam_policy_document" "incr_starter_func_lambda_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "cloudwatchlogspermissions"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:${local.region}:${local.aws_account_id}:*"]
  }
  statement {
    sid = "cloudwatchpermissions"
    actions = [
      "cloudwatch:PutMetricData"
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "dynamodb:Query"

    ]
    resources = [aws_dynamodb_table.incr_control_table.arn

    ]
  }
  statement {
    actions = [
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateNetworkInterface",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcs",
      "ec2:DescribeNetworkInterfaceAttribute",
      "ec2:DescribeNetworkInterfaces"
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRuns"
    ]
    resources = [aws_glue_job.incr_id_res_job.arn,
    aws_glue_job.neptune_loader_job.arn]
  }
  statement {
    actions = [
      "glue:ListJobs"

    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "dynamodb:Query"

    ]
    resources = [aws_dynamodb_table.file_status_table.arn,
      aws_dynamodb_table.neptune_tracking_table.arn
    ]
  }
  statement {
    actions = [
      "kms:Decrypt"

    ]
    resources = [data.aws_kms_key.ddb_key_arn.arn]
  }
}

resource "aws_iam_policy" "incr_starter_func_lambda_policy" {
  name   = "${var.name_prefix}-incr-lambda-starter-func-lambda-policy"
  policy = data.aws_iam_policy_document.incr_starter_func_lambda_policy.json
}


###########################################################
###IAM policy for Neptune glue job lambda starter 
###########################################################

data "aws_iam_policy_document" "neptune_glue_job_lambda_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "cloudwatchlogspermissions"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:${local.region}:${local.aws_account_id}:*"]
  }
  statement {
    sid = "cloudwatchpermissions"
    actions = [
      "cloudwatch:PutMetricData"
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateNetworkInterface",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcs",
      "ec2:DescribeNetworkInterfaceAttribute",
      "ec2:DescribeNetworkInterfaces"
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRuns"
    ]
    resources = [aws_glue_job.neptune_loader_job.arn]
  }
  statement {
    actions = [
      "glue:ListJobs"

    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "dynamodb:Query"

    ]
    resources = [aws_dynamodb_table.neptune_tracking_table.arn]
  }
  statement {
    actions = [
      "kms:Decrypt"

    ]
    resources = [data.aws_kms_key.ddb_key_arn.arn]
  }
}

##########################################################
#### IAM role for API gateway global role
##########################################################


data "aws_iam_policy_document" "api_gw_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["apigateway.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "cloudwatch" {
  name                 = "${var.name_prefix}-api_gateway_cloudwatch_global-${var.environment}"
  assume_role_policy   = data.aws_iam_policy_document.api_gw_assume_role.json
  permissions_boundary = var.permission_boundary_arn
}

data "aws_iam_policy_document" "cloudwatch" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
      "logs:PutLogEvents",
      "logs:GetLogEvents",
      "logs:FilterLogEvents",
    ]

    resources = ["*"]
  }
}
resource "aws_iam_role_policy" "cloudwatch" {
  name   = "${var.name_prefix}-default-${var.environment}"
  role   = aws_iam_role.cloudwatch.id
  policy = data.aws_iam_policy_document.cloudwatch.json
}



# ############################################################
# ##### Openserch Service role
# ############################################################
# External data source to run the bash script
data "external" "iam_role_check" {
  program = ["bash", "${path.module}/check_resource.sh", "AWSServiceRoleForAmazonOpenSearchService"]
}

resource "aws_iam_service_linked_role" "opensearch_service_role" {
  aws_service_name = "opensearchservice.amazonaws.com"
  count            = data.external.iam_role_check.result["exists"] == "false" ? 1 : 0
  depends_on       = [data.external.iam_role_check]
}


# ############################################################
# ##### Snapshot role for Openserch backups
# ############################################################

resource "aws_iam_role" "opensearch_snapshot_role" {
  name                 = "${var.name_prefix}-opensearch-snapshot-role-${var.environment}"
  assume_role_policy   = data.aws_iam_policy_document.opensearch_snapshot_assume_role.json
  permissions_boundary = var.permission_boundary_arn
}

data "aws_iam_policy_document" "opensearch_snapshot_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["es.amazonaws.com"]
    }
    principals {
      type        = "Service"
      identifiers = ["opensearchservice.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = ["${local.aws_account_id}"]
    }
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:aws:es:${local.region}:${local.aws_account_id}:domain/${aws_opensearch_domain.opensearch.domain_name}"]
    }

  }
}



resource "aws_iam_role_policy" "opensearch_snapshot_policy" {
  name   = "${var.name_prefix}-opensearch-snapshot-${var.environment}"
  role   = aws_iam_role.opensearch_snapshot_role.id
  policy = data.aws_iam_policy_document.opensearch_snapshot_policy.json
}

data "aws_iam_policy_document" "opensearch_snapshot_policy" {
  #checkov:skip=CKV_AWS_111: "Ensure IAM policies does not allow write access without constraints"
  #checkov:skip=CKV_AWS_356: "Ensure no IAM policies documents allow "*" as a statement's resource for restrictable actions"
  statement {
    sid = "S3Permissions"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:PutObject",
      "s3:ListBucket",
      "s3:DeleteObject"
    ]
    resources = [
      data.aws_s3_bucket.idres_scratch_space_bucket.arn,
      "${data.aws_s3_bucket.idres_scratch_space_bucket.arn}/*"
    ]
  }
  statement {
    sid = "OpenSearchPermissions"
    actions = [
      "es:Describe*",
      "es:ESHttpGet",
      "es:ESHttpHead",
      "es:ESHttpPut"
    ]
    resources = [aws_opensearch_domain.opensearch.arn]
  }
  statement {
    sid = "cloudwatchpermissions"
    actions = [
      "cloudwatch:PutMetricData"
    ]
    resources = ["*"]
  }
  statement {
    sid = "passrole"
    actions = [
      "iam:PassRole"
    ]
    resources = [aws_iam_role.opensearch_snapshot_role.arn]
  }
}
