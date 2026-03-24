#####################################################
### Glue Database
#####################################################
resource "aws_glue_catalog_database" "common_schema_db" {
  name        = join("-", ["${var.name_prefix}", "common", "${var.environment}"])
  description = "Common Schema Database for input into ER service"
}
#####################################################
### Glue Network Connection
#####################################################
resource "aws_glue_connection" "glue_network_connection" {
  name            = join("-", ["${var.name_prefix}", "network-connection", "${var.environment}"])
  connection_type = "NETWORK"
  physical_connection_requirements {
    availability_zone      = local.private_subnet_info[0].availability_zone
    security_group_id_list = [aws_security_group.glue_connection_sg.id]
    subnet_id              = local.private_subnet_info[0].subnet_id
  }
}

#uploads file to S3 buckets for glue notebook
resource "aws_s3_object" "notebook_executable_code" {
  key    = "terraform/notebooks/udx-cust-mesh-idres-historical-data.ipynb"
  bucket = data.aws_s3_bucket.glue_code_bucket.bucket
  source = "../src/glue_notebooks/udx-cust-mesh-idres-historical-data.ipynb"
  etag   = filemd5("../src/glue_notebooks/udx-cust-mesh-idres-historical-data.ipynb")
}

resource "aws_s3_object" "incremental_notebook_executable_code" {
  key    = "terraform/notebooks/udx-cust-mesh-idres-incremental_id_res_glue_notebook.ipynb"
  bucket = data.aws_s3_bucket.glue_code_bucket.bucket
  source = "../src/operations_script/operations_glue_notebook/incremental_id_res_glue_notebook.ipynb"
  etag   = filemd5("../src/operations_script/operations_glue_notebook/incremental_id_res_glue_notebook.ipynb")
}

data "archive_file" "common_glue_modules_utils_zip" {
  type        = "zip"
  source_dir  = "../src/glue_jobs/glue_modules/"
  output_path = "../src/glue_jobs/glue_modules.zip"
}

resource "aws_s3_object" "common_glue_modules_utils_zip" {
  key         = "terraform/scripts/common-glue_modules/glue_modules.zip"
  bucket      = data.aws_s3_bucket.glue_code_bucket.bucket
  source      = data.archive_file.common_glue_modules_utils_zip.output_path
  source_hash = data.archive_file.common_glue_modules_utils_zip.output_base64sha256
}

#####################################################
### Incremental Spark Job for ER Service
#####################################################

resource "aws_glue_job" "incr_id_res_job" {
  #checkov:skip=CKV_AWS_195: "Ensure Glue component has a security configuration associated"
  name              = join("-", ["${var.name_prefix}", "incr-id-res-job", "${var.environment}"])
  role_arn          = aws_iam_role.glue_id_res_job_role.arn
  worker_type       = var.incr_er_worker_type_sm
  number_of_workers = var.incr_er_glue_worker_sm_count
  glue_version      = var.incr_id_res_job_glue_version
  max_retries       = 0
  connections       = [aws_glue_connection.glue_network_connection.name]
  timeout           = var.incr_id_res_job_timeout
  command {
    name            = "glueetl"
    script_location = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.incr_er_glue_job_file.key}"
    python_version  = 3
  }
  default_arguments = {
    "--conf"                                           = "spark.task.maxFailures=1"
    "--enable-auto-scaling"                            = "false"
    "--job-language"                                   = "python"
    "--enable-glue-datacatalog"                        = "true"
    "--enable-continuous-cloudwatch-log"               = "true"
    "--enable-continuous-log-filter"                   = "true"
    "--enable-metrics"                                 = "true"
    "--TempDir"                                        = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/incr_er_service_temp/"
    "--dydb_table"                                     = "${aws_dynamodb_table.file_status_table.id}"
    "--max_items"                                      = var.max_items
    "--max_records"                                    = var.max_records
    "--expire_in_days"                                 = var.expire_in_days
    "--incremental_data_sources_enabled"               = var.incremental_data_sources_enabled
    "--incremental_common_schema_data_override"        = "not_overridden_use_default" # overrides the transformed data for incremental id res with a single transformed data batch. Used for testing.
    "--incremental_after_pre_ml_rules_path_override"   = "not_overridden_use_default" # overrides the df_batch after application of pre-ml rules to skip over ER run. Used for testing.
    "--incremental_er_results_path_override"           = "not_overridden_use_default" # overrides the transformed data to skip over ER run. Used for testing.
    "--additional-python-modules"                      = "boto3==1.35.18,opensearch-py==2.7.1,requests-aws4auth==1.3.1,requests==2.32.3, urllib3==2.2.3, tenacity==9.0.0"
    "--extra-py-files"                                 = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.common_glue_modules_utils_zip.key},s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.graphframes_jars_files.key}"
    "--extra-jars"                                     = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.graphframes_jars_files.key}"
    "--output_s3_bucket"                               = data.aws_s3_bucket.idres_data_out.bucket
    "--output_s3_url"                                  = data.aws_s3_bucket.idres_temp_bucket.bucket
    "--enable-observability-metrics"                   = "true"
    "--enable-spark-ui"                                = "true"
    "--spark-event-logs-path"                          = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/sparkHistoryLogs/"
    "--role_arn"                                       = aws_iam_role.er_service_role.arn
    "--incremental_data_s3_bucket"                     = data.aws_s3_bucket.idres_data_out.bucket
    "--incremental_glue_db"                            = "${aws_glue_catalog_database.common_schema_db.name}"
    "--incremental_glue_table"                         = "incremental_transformed_data"
    "--blocklist_table_name"                           = aws_dynamodb_table.blocklist_table_name.arn
    "--neptune_dydb_table"                             = aws_dynamodb_table.neptune_tracking_table.name
    "--udx_address_store_table"                        = aws_dynamodb_table.udx_address_store_table.name
    "--temp_s3_bucket"                                 = data.aws_s3_bucket.idres_temp_bucket.bucket
    "--er_input_source_arn"                            = "arn:aws:glue:us-east-1:${local.aws_account_id}:table/${aws_glue_catalog_database.common_schema_db.name}/incremental_transformed_data"
    "--er_service_workflow_name"                       = join("-", ["${var.name_prefix}", "er_ml_incr", "${var.environment}"])
    "--er_service_input_schema_name"                   = join("-", ["${var.name_prefix}", "schema_incr", "${var.environment}"])
    "--ER_IAM_ROLE"                                    = aws_iam_role.er_service_role.arn
    "--schema_mapping_arn"                             = "arn:aws:entityresolution:${local.region}:${local.aws_account_id}:schemamapping/${var.name_prefix}-schema-mappings-${var.environment}"
    "--opensearch_index_name_override"                 = "not_overridden_use_default"
    "--ES_HOST"                                        = "${aws_opensearch_domain.opensearch.endpoint}"
    "--ES_IAM_MASTER_USER_ARN"                         = aws_iam_role.opensearch_master_user_role.arn
    "--ES_REGION"                                      = local.region
    "--NEPTUNE_BULK_ROLE"                              = "${aws_iam_role.neptune_role.arn}"
    "--NEPTUNE_HOST"                                   = "${aws_neptune_cluster.neptune_cluster.endpoint}"
    "--NEPTUNE_PORT"                                   = "${aws_neptune_cluster.neptune_cluster.port}"
    "--NEPTUNE_BATCH_SIZE"                             = var.neptune_batch_size
    "--NEPTUNE_MAX_BULK_ROWS"                          = "1000000"
    "--NEPTUNE_IS_BULK_COMPRESSED"                     = "true"
    "--debug_output_s3"                                = var.debug_output_s3 # if true, writes intermediate dataframes to S3 temp bucket for debugging purposes
    "--ctrl_incr_key"                                  = "${var.ctrl_incr_key}"
    "--incr_ctrl_table"                                = "${aws_dynamodb_table.incr_control_table.id}"
    "--incremental_id_res_circuit_breaker_min_records" = "${var.incremental_id_res_circuit_breaker_min_records}"
    "--incremental_id_res_circuit_breaker_ratio"       = "${var.incremental_id_res_circuit_breaker_ratio}"
    "--reverse_append_preflight_enabled"               = var.reverse_append_preflight_enabled
    "--reverse_append_cache_table"                     = aws_dynamodb_table.reverse_append_email_cache.name
    "--snowflake_jdbc_secret_arn"                      = trimspace(var.snowflake_jdbc_secret_arn) != "" ? var.snowflake_jdbc_secret_arn : "DISABLED"
    "--snowflake_enrichment_table_fqn"                 = var.snowflake_enrichment_table_fqn
    "--snowflake_email_column"                         = var.snowflake_email_column
  }
  execution_property {
    max_concurrent_runs = var.incr_er_max_conc_runs
  }
  depends_on = [aws_s3_object.incr_er_glue_job_file, aws_s3_object.common_glue_modules_utils_zip]
}


resource "aws_s3_object" "incr_er_glue_job_file" {
  key         = "terraform/scripts/incr_id_res_job.py"
  bucket      = data.aws_s3_bucket.glue_code_bucket.bucket
  source      = "../src/glue_jobs/incr_id_res_job/incr_id_res_job.py"
  source_hash = base64encode(file("../src/glue_jobs/incr_id_res_job/incr_id_res_job.py"))

}

#####################################################
### Large Source ETL Spark job
#####################################################
resource "aws_glue_job" "large_source_etl_job" {
  #checkov:skip=CKV_AWS_195: "Ensure Glue component has a security configuration associated"
  name              = join("-", ["${var.name_prefix}", "large-source-etl-glue-job", "${var.environment}"])
  role_arn          = aws_iam_role.large_source_etl_job_role.arn
  worker_type       = var.large_source_worker_type_sm
  number_of_workers = var.large_source_glue_worker_sm_count
  glue_version      = var.large_source_etl_job_glue_version
  max_retries       = 0
  connections       = [aws_glue_connection.glue_network_connection.name]
  timeout           = var.large_source_etl_job_timeout


  command {
    name            = "glueetl"
    script_location = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.large_source_etl_glue_job_file.key}"
    python_version  = 3
  }
  execution_class = "STANDARD"
  default_arguments = {
    "--enable-auto-scaling"              = "false"
    "--job-language"                     = "python"
    "--enable-glue-datacatalog"          = "true"
    "--enable-job-insights"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/large_source_etl_temp/"
    "--s3_output_path"                   = "s3://${data.aws_s3_bucket.idres_data_out.bucket}/transformed/"
    "--dynamo_table_name"                = "${aws_dynamodb_table.file_status_table.id}"
    "--glue_database_name"               = "${aws_glue_catalog_database.common_schema_db.name}"
    "--glue_table_name"                  = var.glue_table_name
    "--additional-python-modules"        = "boto3==1.35.12"
    "--extra-py-files"                   = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.large_source_etl_job_zip.key}"
    "--enable-observability-metrics"     = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/sparkHistoryLogs/"
  }
  execution_property {
    max_concurrent_runs = var.large_source_max_conc_runs
  }
  depends_on = [aws_s3_object.large_source_etl_glue_job_file, aws_s3_object.common_glue_modules_utils_zip]
}

data "archive_file" "large_source_etl_job_zip" {
  type        = "zip"
  source_dir  = "../src/glue_jobs/large_source_etl_job/large_src_etl_utils/"
  output_path = "../src/glue_jobs/large_source_etl_job/large_src_etl_utils.zip"
}

resource "aws_s3_object" "large_source_etl_job_zip" {
  key         = "terraform/scripts/large_source_etl_job/large_src_etl_utils.zip"
  bucket      = data.aws_s3_bucket.glue_code_bucket.bucket
  source      = data.archive_file.large_source_etl_job_zip.output_path
  source_hash = data.archive_file.large_source_etl_job_zip.output_base64sha256
}
resource "aws_s3_object" "large_source_etl_glue_job_file" {
  key    = "terraform/scripts/large_source_etl_job.py"
  bucket = data.aws_s3_bucket.glue_code_bucket.bucket
  source = "../src/glue_jobs/large_source_etl_job/large_source_etl_job.py"
  etag   = filemd5("../src/glue_jobs/large_source_etl_job/large_source_etl_job.py")
}


#####################################################
### Historical glue job Step 1 preml presearch rules
#####################################################
resource "aws_glue_job" "historical_step_1_job" {
  #checkov:skip=CKV_AWS_195: "Ensure Glue component has a security configuration associated"
  name              = join("-", ["${var.name_prefix}", "historical-glue-job-step-1-before-er", "${var.environment}"])
  role_arn          = aws_iam_role.glue_id_res_job_role.arn
  worker_type       = var.historical_step_1_worker_type_sm
  number_of_workers = var.historical_step_1_worker_sm_count
  glue_version      = var.historical_step_1_job_glue_version
  max_retries       = 0
  connections       = [aws_glue_connection.glue_network_connection.name]
  timeout           = var.historical_step_1_job_timeout


  command {
    name            = "glueetl"
    script_location = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.historical_step_1_glue_job_file.key}"
    python_version  = 3
  }
  execution_class = "STANDARD"
  default_arguments = {
    "--enable-auto-scaling"                           = "false"
    "--job-language"                                  = "python"
    "--enable-glue-datacatalog"                       = "true"
    "--enable-job-insights"                           = "true"
    "--enable-continuous-cloudwatch-log"              = "true"
    "--enable-continuous-log-filter"                  = "true"
    "--enable-metrics"                                = "true"
    "--TempDir"                                       = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/historical_step_1_glue_job_temp/"
    "--additional-python-modules"                     = "opensearch-py==2.7.1,pandas==2.2.2,requests-aws4auth==1.3.1,boto3==1.35.18,requests==2.32.3,urllib3==2.2.3"
    "--er_service_workflow_name"                      = join("-", ["${var.name_prefix}", "er_ml_hist", "${var.environment}"])
    "--er_service_input_schema_name"                  = join("-", ["${var.name_prefix}", "schema_hist", "${var.environment}"])
    "--er_input_source_arn"                           = "arn:aws:glue:us-east-1:${local.aws_account_id}:table/${aws_glue_catalog_database.common_schema_db.name}/historical_transformed_data"
    "--opensearch_index_name_override"                = "not_overridden_use_default"
    "--opensearch_historical_writer_partitions_count" = "${var.opensearch_historical_1_writer_partitions_count}"
    "--id_res_job_id"                                 = "RESERVED"
    "--historical_job_er_service_job_id"              = "RESERVED"
    "--historical_data_s3_path"                       = "SET_MANUALLY"
    "--historical_data_s3_format"                     = "SET_MANUALLY_TO_csv_OR_parquet" # Valid types: csv/parquet
    "--historical_glue_db"                            = "${aws_glue_catalog_database.common_schema_db.name}"
    "--historical_glue_table"                         = "historical_transformed_data"
    "--blocklist_table_name"                          = aws_dynamodb_table.blocklist_table_name.arn
    "--extra-py-files"                                = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.common_glue_modules_utils_zip.key}"
    "--enable-observability-metrics"                  = "true"
    "--enable-spark-ui"                               = "true"
    "--spark-event-logs-path"                         = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/sparkHistoryLogs/"
    "--historical_data_s3_bucket"                     = data.aws_s3_bucket.historical_data_bucket.bucket
    "--temp_s3_bucket"                                = data.aws_s3_bucket.idres_temp_bucket.bucket
    "--ES_HOST"                                       = "${aws_opensearch_domain.opensearch.endpoint}"
    "--ES_IAM_MASTER_USER_ARN"                        = aws_iam_role.opensearch_master_user_role.arn
    "--ES_REGION"                                     = local.region
    "--ER_IAM_ROLE"                                   = aws_iam_role.er_service_role.arn
    "--NEPTUNE_BULK_ROLE"                             = "${aws_iam_role.neptune_role.arn}"
    "--NEPTUNE_HOST"                                  = "${aws_neptune_cluster.neptune_cluster.endpoint}"
    "--NEPTUNE_PORT"                                  = "${aws_neptune_cluster.neptune_cluster.port}"
    "--NEPTUNE_MAX_BULK_ROWS"                         = "1000000"
    "--SNOWFLAKE_EXPORT_S3_INPUT_PATH"                = "RESERVED"
    "--NEPTUNE_IS_BULK_COMPRESSED"                    = "true"
    "--debug_output_s3"                               = var.debug_output_s3 # if true, writes intermediate dataframes to S3 temp bucket for debugging purposes
  }
  execution_property {
    max_concurrent_runs = var.historical_step_1_max_conc_runs
  }
  depends_on = [aws_s3_object.historical_step_1_glue_job_file, aws_s3_object.common_glue_modules_utils_zip]
}

resource "aws_s3_object" "historical_step_1_glue_job_file" {
  key    = "terraform/scripts/historical_job_step_1.py"
  bucket = data.aws_s3_bucket.glue_code_bucket.bucket
  source = "../src/glue_jobs/historical_step_1_job/historical_job_step_1.py"
  etag   = filemd5("../src/glue_jobs/historical_step_1_job/historical_job_step_1.py")
}


#####################################################
### Historical glue job Step 2 postml rules
#####################################################
resource "aws_glue_job" "historical_step_2_job" {
  #checkov:skip=CKV_AWS_195: "Ensure Glue component has a security configuration associated"
  name              = join("-", ["${var.name_prefix}", "historical-glue-job-step-2-postml-rules", "${var.environment}"])
  role_arn          = aws_iam_role.glue_id_res_job_role.arn
  worker_type       = var.historical_step_2_worker_type_sm
  number_of_workers = var.historical_step_2_worker_sm_count
  glue_version      = var.historical_step_2_job_glue_version
  max_retries       = 0
  connections       = [aws_glue_connection.glue_network_connection.name]
  timeout           = var.historical_step_2_job_timeout

  command {
    name            = "glueetl"
    script_location = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.historical_step_2_glue_job_file.key}"
    python_version  = 3
  }
  execution_class = "STANDARD"
  default_arguments = {
    "--enable-auto-scaling"                           = "false"
    "--job-language"                                  = "python"
    "--enable-glue-datacatalog"                       = "true"
    "--enable-job-insights"                           = "true"
    "--enable-continuous-cloudwatch-log"              = "true"
    "--enable-continuous-log-filter"                  = "true"
    "--enable-metrics"                                = "true"
    "--er_service_workflow_name"                      = join("-", ["${var.name_prefix}", "er_ml_hist", "${var.environment}"])
    "--er_service_input_schema_name"                  = join("-", ["${var.name_prefix}", "schema_hist", "${var.environment}"])
    "--er_input_source_arn"                           = "RESERVED"
    "--opensearch_index_name_override"                = "not_overridden_use_default"
    "--opensearch_historical_writer_partitions_count" = "${var.opensearch_historical_2_writer_partitions_count}"
    "--id_res_job_id"                                 = "SET_MANUALLY"
    "--historical_job_er_service_job_id"              = "SET_MANUALLY"
    "--historical_data_s3_path"                       = "RESERVED",
    "--historical_data_s3_format"                     = "RESERVED"
    "--historical_glue_db"                            = "${aws_glue_catalog_database.common_schema_db.name}",
    "--historical_glue_table"                         = "historical_transformed_data",
    "--blocklist_table_name"                          = aws_dynamodb_table.blocklist_table_name.arn
    "--TempDir"                                       = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/historical_step_2_glue_job_temp/"
    "--additional-python-modules"                     = "opensearch-py==2.7.1,pandas==2.2.2,requests-aws4auth==1.3.1,boto3==1.35.18,requests==2.32.3,urllib3==2.2.3"
    "--extra-py-files"                                = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.common_glue_modules_utils_zip.key},s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.graphframes_jars_files.key}"
    "--extra-jars"                                    = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.graphframes_jars_files.key}"
    "--output_s3_bucket"                              = "RESERVED"
    "--enable-observability-metrics"                  = "true"
    "--enable-spark-ui"                               = "true"
    "--spark-event-logs-path"                         = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/sparkHistoryLogs/"
    "--historical_data_s3_bucket"                     = data.aws_s3_bucket.historical_data_bucket.bucket
    "--temp_s3_bucket"                                = data.aws_s3_bucket.idres_temp_bucket.bucket,
    "--ES_HOST"                                       = "${aws_opensearch_domain.opensearch.endpoint}"
    "--ES_IAM_MASTER_USER_ARN"                        = aws_iam_role.opensearch_master_user_role.arn
    "--ES_REGION"                                     = local.region
    "--ER_IAM_ROLE"                                   = aws_iam_role.er_service_role.arn
    "--NEPTUNE_BULK_ROLE"                             = "${aws_iam_role.neptune_role.arn}"
    "--NEPTUNE_HOST"                                  = "${aws_neptune_cluster.neptune_cluster.endpoint}"
    "--NEPTUNE_PORT"                                  = "${aws_neptune_cluster.neptune_cluster.port}"
    "--NEPTUNE_MAX_BULK_ROWS"                         = "1000000"
    "--NEPTUNE_IS_BULK_COMPRESSED"                    = "true"
    "--SNOWFLAKE_EXPORT_S3_INPUT_PATH"                = "RESERVED"
    "--debug_output_s3"                               = var.debug_output_s3 # if true, writes intermediate dataframes to S3 temp bucket for debugging purposes

  }
  execution_property {
    max_concurrent_runs = var.historical_step_2_max_conc_runs
  }
}

resource "aws_s3_object" "historical_step_2_glue_job_file" {
  key    = "terraform/scripts/historical_job_step_2.py"
  bucket = data.aws_s3_bucket.glue_code_bucket.bucket
  source = "../src/glue_jobs/historical_step_2_job/historical_job_step_2.py"
  etag   = filemd5("../src/glue_jobs/historical_step_2_job/historical_job_step_2.py")
}

resource "aws_s3_object" "graphframes_jars_files" {
  key    = "terraform/scripts/graphframes-0.8.2-spark3.2-s_2.12.jar"
  bucket = data.aws_s3_bucket.glue_code_bucket.bucket
  source = "../src/glue_jobs/historical_step_2_job/graphframes-0.8.2-spark3.2-s_2.12.jar"
  etag   = filemd5("../src/glue_jobs/historical_step_2_job/graphframes-0.8.2-spark3.2-s_2.12.jar")
}


#####################################################
### Historical glue job Step 3 opensearch load
#####################################################
resource "aws_glue_job" "historical_step_3_job" {
  #checkov:skip=CKV_AWS_195: "Ensure Glue component has a security configuration associated"
  name              = join("-", ["${var.name_prefix}", "historical-glue-job-step-3-opensearch", "${var.environment}"])
  role_arn          = aws_iam_role.glue_id_res_job_role.arn
  worker_type       = var.historical_step_3_worker_type_sm
  number_of_workers = var.historical_step_3_worker_sm_count
  glue_version      = var.historical_step_3_job_glue_version
  max_retries       = 0
  connections       = [aws_glue_connection.glue_network_connection.name]
  timeout           = var.historical_step_3_job_timeout

  command {
    name            = "glueetl"
    script_location = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.historical_step_3_glue_job_file.key}"
    python_version  = 3
  }
  execution_class = "STANDARD"
  default_arguments = {
    "--enable-auto-scaling"                           = "false"
    "--job-language"                                  = "python"
    "--enable-glue-datacatalog"                       = "true"
    "--enable-job-insights"                           = "true"
    "--enable-continuous-cloudwatch-log"              = "true"
    "--enable-continuous-log-filter"                  = "true"
    "--enable-metrics"                                = "true"
    "--er_service_workflow_name"                      = join("-", ["${var.name_prefix}", "er_ml_hist", "${var.environment}"])
    "--er_service_input_schema_name"                  = join("-", ["${var.name_prefix}", "schema_hist", "${var.environment}"])
    "--er_input_source_arn"                           = "RESERVED"
    "--opensearch_index_name_override"                = "not_overridden_use_default"
    "--opensearch_historical_writer_partitions_count" = var.opensearch_historical_3_writer_partitions_count
    "--id_res_job_id"                                 = "SET_MANUALLY"
    "--historical_job_er_service_job_id"              = "SET_MANUALLY"
    "--historical_data_s3_path"                       = "RESERVED",
    "--historical_data_s3_format"                     = "RESERVED"
    "--historical_glue_db"                            = "${aws_glue_catalog_database.common_schema_db.name}",
    "--historical_glue_table"                         = "historical_transformed_data",
    "--blocklist_table_name"                          = aws_dynamodb_table.blocklist_table_name.arn
    "--TempDir"                                       = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/historical_step_3_glue_job_temp/"
    "--additional-python-modules"                     = "opensearch-py==2.7.1,pandas==2.2.2,requests-aws4auth==1.3.1,boto3==1.35.18,requests==2.32.3,urllib3==2.2.3"
    "--extra-py-files"                                = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.common_glue_modules_utils_zip.key}"
    "--output_s3_bucket"                              = "RESERVED"
    "--enable-observability-metrics"                  = "true"
    "--enable-spark-ui"                               = "true"
    "--spark-event-logs-path"                         = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/sparkHistoryLogs/"
    "--historical_data_s3_bucket"                     = data.aws_s3_bucket.historical_data_bucket.bucket
    "--temp_s3_bucket"                                = data.aws_s3_bucket.idres_temp_bucket.bucket,
    "--ES_HOST"                                       = "${aws_opensearch_domain.opensearch.endpoint}"
    "--ES_IAM_MASTER_USER_ARN"                        = aws_iam_role.opensearch_master_user_role.arn
    "--ES_REGION"                                     = local.region
    "--ER_IAM_ROLE"                                   = aws_iam_role.er_service_role.arn
    "--NEPTUNE_BULK_ROLE"                             = "${aws_iam_role.neptune_role.arn}"
    "--NEPTUNE_HOST"                                  = "${aws_neptune_cluster.neptune_cluster.endpoint}"
    "--NEPTUNE_PORT"                                  = "${aws_neptune_cluster.neptune_cluster.port}"
    "--NEPTUNE_MAX_BULK_ROWS"                         = "1000000"
    "--NEPTUNE_IS_BULK_COMPRESSED"                    = "true"
    "--SNOWFLAKE_EXPORT_S3_INPUT_PATH"                = "SET_MANUALLY"
    "--debug_output_s3"                               = var.debug_output_s3 # if true, writes intermediate dataframes to S3 temp bucket for debugging purposes
  }
  execution_property {
    max_concurrent_runs = var.historical_step_3_max_conc_runs
  }
}

resource "aws_s3_object" "historical_step_3_glue_job_file" {
  key    = "terraform/scripts/historical_job_step_3.py"
  bucket = data.aws_s3_bucket.glue_code_bucket.bucket
  source = "../src/glue_jobs/historical_step_3_job/historical_job_step_3.py"
  etag   = filemd5("../src/glue_jobs/historical_step_3_job/historical_job_step_3.py")
}

#####################################################
### Historical glue job Step 4 Neptune Load
#####################################################
resource "aws_glue_job" "historical_step_4_job" {
  #checkov:skip=CKV_AWS_195: "Ensure Glue component has a security configuration associated"
  name              = join("-", ["${var.name_prefix}", "historical-glue-job-step-4-neptune", "${var.environment}"])
  role_arn          = aws_iam_role.glue_id_res_job_role.arn
  worker_type       = var.historical_step_4_worker_type_sm
  number_of_workers = var.historical_step_4_worker_sm_count
  glue_version      = var.historical_step_4_job_glue_version
  max_retries       = 0
  connections       = [aws_glue_connection.glue_network_connection.name]
  timeout           = var.historical_step_4_job_timeout

  command {
    name            = "glueetl"
    script_location = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.historical_step_4_glue_job_file.key}"
    python_version  = 3
  }
  execution_class = "STANDARD"
  default_arguments = {
    "--enable-auto-scaling"                           = "false"
    "--job-language"                                  = "python"
    "--enable-glue-datacatalog"                       = "true"
    "--enable-job-insights"                           = "true"
    "--enable-continuous-cloudwatch-log"              = "true"
    "--enable-continuous-log-filter"                  = "true"
    "--enable-metrics"                                = "true"
    "--er_service_workflow_name"                      = join("-", ["${var.name_prefix}", "er_ml_hist", "${var.environment}"])
    "--er_service_input_schema_name"                  = join("-", ["${var.name_prefix}", "schema_hist", "${var.environment}"])
    "--er_input_source_arn"                           = "RESERVED"
    "--opensearch_index_name_override"                = "not_overridden_use_default"
    "--opensearch_historical_writer_partitions_count" = "${var.opensearch_historical_4_writer_partitions_count}"
    "--id_res_job_id"                                 = "SET_MANUALLY"
    "--historical_job_er_service_job_id"              = "SET_MANUALLY"
    "--historical_data_s3_path"                       = "RESERVED",
    "--historical_data_s3_format"                     = "RESERVED"
    "--historical_glue_db"                            = "${aws_glue_catalog_database.common_schema_db.name}",
    "--historical_glue_table"                         = "historical_transformed_data",
    "--blocklist_table_name"                          = aws_dynamodb_table.blocklist_table_name.arn
    "--TempDir"                                       = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/historical_step_4_glue_job_temp/"
    "--additional-python-modules"                     = "opensearch-py==2.7.1,pandas==2.2.2,requests-aws4auth==1.3.1,boto3==1.35.18,requests==2.32.3,urllib3==2.2.3"
    "--extra-py-files"                                = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.common_glue_modules_utils_zip.key}"
    "--enable-observability-metrics"                  = "true"
    "--enable-spark-ui"                               = "true"
    "--output_s3_bucket"                              = "RESERVED"
    "--spark-event-logs-path"                         = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/sparkHistoryLogs/"
    "--historical_data_s3_bucket"                     = data.aws_s3_bucket.historical_data_bucket.bucket
    "--temp_s3_bucket"                                = data.aws_s3_bucket.idres_temp_bucket.bucket,
    "--ES_HOST"                                       = "${aws_opensearch_domain.opensearch.endpoint}"
    "--ES_IAM_MASTER_USER_ARN"                        = aws_iam_role.opensearch_master_user_role.arn
    "--ES_REGION"                                     = local.region
    "--ER_IAM_ROLE"                                   = aws_iam_role.er_service_role.arn
    "--NEPTUNE_BULK_ROLE"                             = "${aws_iam_role.neptune_role.arn}"
    "--NEPTUNE_HOST"                                  = "${aws_neptune_cluster.neptune_cluster.endpoint}"
    "--NEPTUNE_PORT"                                  = "${aws_neptune_cluster.neptune_cluster.port}"
    "--NEPTUNE_MAX_BULK_ROWS"                         = "1000000"
    "--NEPTUNE_IS_BULK_COMPRESSED"                    = "true"
    "--SNOWFLAKE_EXPORT_S3_INPUT_PATH"                = "SET_MANUALLY"
    "--debug_output_s3"                               = var.debug_output_s3 # if true, writes intermediate dataframes to S3 temp bucket for debugging purposes
  }
  execution_property {
    max_concurrent_runs = var.historical_step_4_max_conc_runs
  }
}

resource "aws_s3_object" "historical_step_4_glue_job_file" {
  key    = "terraform/scripts/historical_job_step_4.py"
  bucket = data.aws_s3_bucket.glue_code_bucket.bucket
  source = "../src/glue_jobs/historical_step_4_job/historical_job_step_4.py"
  etag   = filemd5("../src/glue_jobs/historical_step_4_job/historical_job_step_4.py")
}

#####################################################
### Decoupled Neptune Loader Job
#####################################################
resource "aws_glue_job" "neptune_loader_job" {
  #checkov:skip=CKV_AWS_195: "Ensure Glue component has a security configuration associated"
  name              = join("-", ["${var.name_prefix}", "neptune-loader-job", "${var.environment}"])
  role_arn          = aws_iam_role.glue_id_res_job_role.arn
  worker_type       = var.neptune_loader_job_worker_type_sm
  number_of_workers = var.neptune_loader_job_worker_sm_count
  glue_version      = var.neptune_loader_job_glue_version
  max_retries       = 0
  connections       = [aws_glue_connection.glue_network_connection.name]
  timeout           = var.neptune_loader_job_timeout

  command {
    name            = "glueetl"
    script_location = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.neptune_loader_job_file.key}"
    python_version  = 3
  }
  execution_class = "STANDARD"
  default_arguments = {
    "--conf"                             = "spark.task.maxFailures=1"
    "--enable-auto-scaling"              = "false"
    "--job-language"                     = "python"
    "--enable-glue-datacatalog"          = "true"
    "--enable-job-insights"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = "true"
    "--id_res_job_id"                    = "SET_MANUALLY"
    "--neptune_dydb_table"               = aws_dynamodb_table.neptune_tracking_table.name
    "--TempDir"                          = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/neptune_loader_job_temp/"
    "--additional-python-modules"        = "opensearch-py==2.7.1,pandas==2.2.2,requests-aws4auth==1.3.1,boto3==1.35.18,requests==2.32.3,urllib3==2.2.3"
    "--extra-py-files"                   = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.common_glue_modules_utils_zip.key}"
    "--enable-observability-metrics"     = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/sparkHistoryLogs/"
    "--historical_data_s3_bucket"        = data.aws_s3_bucket.historical_data_bucket.bucket
    "--temp_s3_bucket"                   = data.aws_s3_bucket.idres_temp_bucket.bucket,
    "--ES_HOST"                          = "${aws_opensearch_domain.opensearch.endpoint}"
    "--ES_IAM_MASTER_USER_ARN"           = aws_iam_role.opensearch_master_user_role.arn
    "--ES_REGION"                        = local.region
    "--ER_IAM_ROLE"                      = aws_iam_role.er_service_role.arn
    "--NEPTUNE_BULK_ROLE"                = "${aws_iam_role.neptune_role.arn}"
    "--NEPTUNE_HOST"                     = "${aws_neptune_cluster.neptune_cluster.endpoint}"
    "--NEPTUNE_PORT"                     = "${aws_neptune_cluster.neptune_cluster.port}"
    "--NEPTUNE_MAX_BULK_ROWS"            = "1000000"
    "--NEPTUNE_IS_BULK_COMPRESSED"       = "true"
    "--NEPTUNE_BATCH_SIZE"               = var.neptune_batch_size
    "--SNOWFLAKE_EXPORT_S3_INPUT_PATH"   = "SET_MANUALLY"
    "--dydb_neptune_file_status_table"   = "${aws_dynamodb_table.neptune_tracking_table.id}"
    "--expire_in_days"                   = var.expire_in_days
    "--ctrl_neptune_key"                 = "${var.ctrl_neptune_key}"
    "--incr_ctrl_table"                  = "${aws_dynamodb_table.incr_control_table.id}"
  }
  execution_property {
    max_concurrent_runs = var.neptune_loader_job_max_conc_runs
  }
}
resource "aws_s3_object" "neptune_loader_job_file" {
  key    = "terraform/scripts/incr_id_res_neptune.py"
  bucket = data.aws_s3_bucket.glue_code_bucket.bucket
  source = "../src/glue_jobs/incr_id_res_neptune/incr_id_res_neptune.py"
  etag   = filemd5("../src/glue_jobs/incr_id_res_neptune/incr_id_res_neptune.py")
}


##############################################################
### Glue job to copy old s3 data to new bucket - ONE TIME JOB
##############################################################
resource "aws_glue_job" "s3_move_job" {
  #checkov:skip=CKV_AWS_195: "Ensure Glue component has a security configuration associated"
  name              = join("-", ["${var.name_prefix}", "s3-move-job", "${var.environment}"])
  role_arn          = aws_iam_role.glue_id_res_job_role.arn
  worker_type       = var.s3_move_job_worker_type_sm
  number_of_workers = var.s3_move_job_worker_sm_count
  glue_version      = var.s3_move_job_glue_version
  max_retries       = 0
  connections       = [aws_glue_connection.glue_network_connection.name]
  timeout           = var.s3_move_job_timeout

  command {
    name            = "glueetl"
    script_location = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.s3_move_job_file.key}"
    python_version  = 3
  }
  execution_class = "STANDARD"
  default_arguments = {
    "--conf"                                = "spark.task.maxFailures=1"
    "--enable-auto-scaling"                 = "false"
    "--job-language"                        = "python"
    "--enable-glue-datacatalog"             = "true"
    "--enable-job-insights"                 = "true"
    "--enable-continuous-cloudwatch-log"    = "true"
    "--enable-continuous-log-filter"        = "true"
    "--enable-metrics"                      = "true"
    "--id_res_job_id"                       = "SET_MANUALLY"
    "--TempDir"                             = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/s3_move_job_temp/"
    "--additional-python-modules"           = "opensearch-py==2.7.1,pandas==2.2.2,requests-aws4auth==1.3.1,boto3==1.35.18,requests==2.32.3,urllib3==2.2.3"
    "--extra-py-files"                      = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.common_glue_modules_utils_zip.key}"
    "--enable-observability-metrics"        = "true"
    "--enable-spark-ui"                     = "true"
    "--spark-event-logs-path"               = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/sparkHistoryLogs/"
    "--historical_data_s3_bucket"           = data.aws_s3_bucket.historical_data_bucket.bucket
    "--temp_s3_bucket"                      = data.aws_s3_bucket.idres_temp_bucket.bucket,
    "--ES_HOST"                             = aws_opensearch_domain.opensearch.endpoint
    "--ES_IAM_MASTER_USER_ARN"              = aws_iam_role.opensearch_master_user_role.arn
    "--ES_REGION"                           = local.region
    "--SNOWFLAKE_EXPORT_S3_INPUT_PATH"      = "SET_MANUALLY"
    "--source_folder"                       = "s3://${data.aws_s3_bucket.idres_scratch_space_bucket.bucket}/ragav_approved_backup/"
    "--source_exclude_comma_delimited_list" = "not_overridden_use_default"
    "--destination_folder"                  = "s3://${data.aws_s3_bucket.idres_data_in.bucket}/"
    "--large_src_job_name"                  = aws_glue_job.large_source_etl_job.name
    "--max_job_concurrency"                 = var.s3_move_job_large_src_concurrency
  }
  execution_property {
    max_concurrent_runs = var.s3_move_job_max_conc_runs
  }
  depends_on = [aws_s3_object.s3_move_job_file]
}
resource "aws_s3_object" "s3_move_job_file" {
  key    = "terraform/scripts/move_incremental_sources.py"
  bucket = data.aws_s3_bucket.glue_code_bucket.bucket
  source = "../src/glue_jobs/s3_move_incremental_job/move_incremental_sources.py"
  etag   = filemd5("../src/glue_jobs/s3_move_incremental_job/move_incremental_sources.py")
}

#####################################################
### Reference Load Shell job
#####################################################
resource "aws_glue_job" "ref_source_load_job" {
  #checkov:skip=CKV_AWS_195: "Ensure Glue component has a security configuration associated"
  name              = join("-", ["${var.name_prefix}", "ref-source-load-job", "${var.environment}"])
  role_arn          = aws_iam_role.large_source_etl_job_role.arn
  connections       = [aws_glue_connection.glue_network_connection.name]

  command {
    name            = "pythonshell"
    script_location = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.ref_source_load_job_file.key}"
    python_version  = "3.9"
  }
  execution_class = "STANDARD"
  default_arguments = {
    "--job-language"                     = "python"
    "--enable-job-insights"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = "true"
    "--bucket_name"                      = "${data.aws_s3_bucket.idres_data_in.bucket}"
    "--blocklist_table_name"             = aws_dynamodb_table.blocklist_table_name.name
    "--additional-python-modules"        = "boto3==1.35.12"
  }

}
resource "aws_s3_object" "ref_source_load_job_file" {
  key    = "terraform/scripts/ref_source_load_job.py"
  bucket = data.aws_s3_bucket.glue_code_bucket.bucket
  source = "../src/glue_jobs/ref_source_load_job/ref_source_load_job.py"
  etag   = filemd5("../src/glue_jobs/ref_source_load_job/ref_source_load_job.py")
}

#####################################################
### UXID unlinking
#####################################################
resource "aws_s3_object" "uxid_unlinking_job_file" {

  key    = "terraform/scripts/uxid_unlinking_job.py"
  bucket = data.aws_s3_bucket.glue_code_bucket.bucket

  source = "../src/glue_jobs/uxid_unlinking/uxid_unlinking_job.py"

  etag = filemd5("../src/glue_jobs/uxid_unlinking/uxid_unlinking_job.py")

}

#####################################################
### UXID unlinking Glue Job
#####################################################
resource "aws_glue_job" "uxid_unlinking_job" {

  name         = join("-", ["${var.name_prefix}", "uxid-unlinking-job", "${var.environment}"])
  role_arn     = aws_iam_role.glue_id_res_job_role.arn
  glue_version = "4.0"

  max_retries = 0
  timeout     = 60

  connections = [aws_glue_connection.glue_network_connection.name]

  command {
    name            = "glueetl"
    script_location = "s3://${data.aws_s3_bucket.glue_code_bucket.bucket}/${aws_s3_object.uxid_unlinking_job_file.key}"
    python_version  = 3
  }

  execution_class = "STANDARD"

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-glue-datacatalog"          = "true"
    "--enable-job-insights"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = "true"

    "--TempDir" = "s3://${data.aws_s3_bucket.idres_temp_bucket.bucket}/uxid-unlinking-temp/"

    "--additional-python-modules" = "boto3==1.35.18,pandas==2.2.2,pyarrow==16.1.0,opensearch-py==2.7.1,requests-aws4auth==1.3.1"

    "--name_prefix" = var.name_prefix
    "--environment" = var.environment

    "--temp_s3_bucket"   = data.aws_s3_bucket.idres_scratch_space_bucket.bucket
    "--output_s3_bucket" = data.aws_s3_bucket.idres_data_out.bucket

    "--INDEX_NAME" = "idstore_1"

    "--ES_HOST"                = aws_opensearch_domain.opensearch.endpoint
    "--ES_REGION"              = local.region
    "--ES_IAM_MASTER_USER_ARN" = aws_iam_role.opensearch_master_user_role.arn

    "--DYNAMODB_TABLE" = aws_dynamodb_table.neptune_tracking_table.name
  }

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [
    aws_s3_object.uxid_unlinking_job_file
  ]
}