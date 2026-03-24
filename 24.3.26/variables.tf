
variable "cluster_configs" {
  description = "Predefined configurations for small, medium, and large clusters"
  type = map(object({

    instance_type         = string
    instance_count        = number
    ebs_volume_size       = string
    throughput            = number
    opensearch_iops       = number
    dedicated_master_type = string
  }))
  default = {

    "small" = {
      instance_type         = "r7g.large.search"
      instance_count        = 3
      ebs_volume_size       = "150"
      throughput            = 125
      opensearch_iops       = 3000
      dedicated_master_type = "m7g.large.search"
    },
    "medium" = {
      instance_type         = "c7g.2xlarge.search"
      instance_count        = 6
      ebs_volume_size       = "200"
      throughput            = 250
      opensearch_iops       = 3000
      dedicated_master_type = "m7g.large.search"
    },
    "large" = {
      instance_type         = "c7g.2xlarge.search"
      instance_count        = 9
      ebs_volume_size       = "250"
      throughput            = 250
      opensearch_iops       = 4500
      dedicated_master_type = "m7g.xlarge.search"
    },
    "extra_large" = {
      instance_type         = "c7g.2xlarge.search"
      instance_count        = 12
      ebs_volume_size       = "300"
      throughput            = 250
      opensearch_iops       = 4500
      dedicated_master_type = "m7g.xlarge.search"
    },
    "2x_large" = {
      instance_type         = "c7g.4xlarge.search"
      instance_count        = 12
      ebs_volume_size       = "300"
      throughput            = 250
      opensearch_iops       = 4500
      dedicated_master_type = "m7g.xlarge.search"
    }
  }
}


variable "name_prefix" {
  description = "Name of prefix for aws resources"
  type        = string
  #default     = "udx-cust-mesh-d"
}

variable "neptune_version_number" {
  description = "version for neptune cluster"
  type        = string
}

variable "environment" {
  description = "Name of environment. dev/qa/preprod/prod"
  type        = string
  default     = "dev"
}

variable "idres_vpc_id" {
  description = "VPC ID"
  type        = string
}


variable "idres_scratch_space_bucket" {
  type = string
}
variable "idres_data_out" {
  type = string
}
variable "idres_temp_bucket" {
  type = string
}
variable "idres_data_in" {
  type = string
}
variable "glue_code_bucket" {
  type = string
}
variable "historical_data_bucket" {
  type = string
}

# variable "idres_neptune_data_bucket" {
#   type = string
# }

variable "incr_er_worker_type_sm" {
  description = "worker type for glue jobs"
  type        = string
}
variable "incr_er_glue_worker_sm_count" {
  description = "number of workers for glue job"
  type        = string
}
variable "incr_er_max_conc_runs" {
  description = "increment er glue jobmaximum concurrent runs for glue job"
  type        = string
}
variable "large_source_worker_type_sm" {
  description = "worker type for glue jobs"
  type        = string
}
variable "large_source_glue_worker_sm_count" {
  description = "number of workers for glue job"
  type        = string
}
variable "large_source_max_conc_runs" {
  description = "large source glue job maximum concurrent runs for glue job"
  type        = string
}

variable "historical_step_1_worker_type_sm" {
  description = "worker type for glue jobs"
  type        = string
}
variable "historical_step_1_worker_sm_count" {
  description = "number of workers for glue job"
  type        = string
}
variable "historical_step_1_max_conc_runs" {
  description = "historical step 1 glue job maximum concurrent runs for glue job"
  type        = string
}
variable "opensearch_historical_1_writer_partitions_count" {
  description = "Number of Spark partitions to use when writing to OpenSearch for historical id job 1 resolution"
  type        = string
}
variable "historical_step_2_worker_type_sm" {
  description = "historical step 2 worker type for glue jobs"
  type        = string
}
variable "historical_step_2_worker_sm_count" {
  description = " historical step 2 number of workers for glue job"
  type        = string
}
variable "historical_step_2_max_conc_runs" {
  description = "historical step 2 glue job maximum concurrent runs for glue job"
  type        = string
}
variable "opensearch_historical_2_writer_partitions_count" {
  description = "Number of Spark partitions to use when writing to OpenSearch for historical id job 2 resolution"
  type        = string
}
variable "historical_step_3_worker_type_sm" {
  description = "historical step 3 worker type for glue jobs"
  type        = string
}
variable "historical_step_3_worker_sm_count" {
  description = " historical step 3 number of workers for glue job"
  type        = string
}
variable "historical_step_3_max_conc_runs" {
  description = "historical step 3 glue job maximum concurrent runs for glue job"
  type        = string
}
variable "opensearch_historical_3_writer_partitions_count" {
  description = "Number of Spark partitions to use when writing to OpenSearch for historical id job 1 resolution"
  type        = string
}

variable "historical_step_4_worker_type_sm" {
  description = "historical step 4 worker type for glue jobs"
  type        = string
}
variable "historical_step_4_worker_sm_count" {
  description = " historical step 4 number of workers for glue job"
  type        = string
}
variable "historical_step_4_max_conc_runs" {
  description = "historical step 4 glue job maximum concurrent runs for glue job"
  type        = string
}

variable "neptune_loader_job_worker_type_sm" {
  description = "neptune loader worker type for glue jobs"
  type        = string
}
variable "neptune_loader_job_worker_sm_count" {
  description = "neptune loader number of workers for glue job"
  type        = string
}
variable "neptune_loader_job_max_conc_runs" {
  description = "neptune loader job maximum concurrent runs for glue job"
  type        = string
}

variable "s3_move_job_worker_type_sm" {
  description = "s3 copy job worker type for glue jobs"
  type        = string
}
variable "s3_move_job_worker_sm_count" {
  description = "s3 copy  number of workers for glue job"
  type        = string
}
variable "s3_move_job_max_conc_runs" {
  description = "s3 copy jobmaximum concurrent runs for glue job"
  type        = string
}






variable "opensearch_historical_4_writer_partitions_count" {
  description = "Number of Spark partitions to use when writing to OpenSearch for historical id job 2 resolution"
  type        = string
}

variable "incr_id_res_job_timeout" {
  description = "incr id res job timeout"
  type        = string
}

variable "large_source_etl_job_timeout" {
  description = "large source etl job timeout"
  type        = string
}
variable "historical_step_1_job_timeout" {
  description = "historical step 1 jobtimeout"
  type        = string
}
variable "historical_step_2_job_timeout" {
  description = "historical step 2 jobtimeout"
  type        = string
}
variable "historical_step_3_job_timeout" {
  description = "historical step 3 jobtimeout"
  type        = string
}
variable "historical_step_4_job_timeout" {
  description = "historical step 4 jobtimeout"
  type        = string
}
variable "neptune_loader_job_timeout" {
  description = "neptune loader jobtimeout"
  type        = string
}

variable "s3_move_job_timeout" {
  description = "s3 copy jobtimeout"
  type        = string
}

variable "permission_boundary_arn" {
  description = "permission boundary arn"
  type        = string
}
variable "neptune_min_size" {
  description = "minimum nodes in neptune cluster"
  type        = string
}
variable "neptune_max_size" {
  description = "maximum nodes in neptune cluster"
  type        = string
}

variable "neptune_kms_key" {
  description = "kms key arn for neptune"
  type        = string
}

variable "neptune_subnet_group_name" {
  description = "neptune subnet group name"
  type        = string
}

variable "opensearch_kms_key" {
  description = "kms key arn for opensearch"
  type        = string
}

variable "ddb_kms_key" {
  description = "kms key arn for Dynamo DB"
  type        = string
}

# variable "s3_kms_key" {
#   description = "kms key arn for "
#   type        = string
# }
variable "sqs_sns_kms_key" {
  description = "kms key arn for SQS and SNS"
  type        = string
}

variable "file_status_maximum_retry_attempts" {
  description = "file status maximum retry attempts"
  type        = string
}
variable "small_etl_maximum_retry_attempts" {
  description = "small etl maximum retry attempts"
  type        = string
}
variable "incr_glue_lambda_maximum_retry_attempts" {
  description = "incr glue maximum retry attempts"
  type        = string
}
variable "search_func_lambda_maximum_retry_attempts" {
  description = "search func lambda maximum retry attempts"
  type        = string
}
variable "incr_jobs_starter_lambda_maximum_retry_attempts" {
  description = "search func lambda maximum retry attempts"
  type        = string
  default     = "0"
}
variable "neptune_job_starter_lambda_maximum_retry_attempts" {
  description = "search func lambda maximum retry attempts"
  type        = string
  default     = "0"
}
variable "incr_job_starter_enablement" {
  description = "incr job starter enablement"
  type        = string
  default     = "DISABLED"
}

variable "ms_guests_valid_sources" {
  description = "incr job starter enablement"
  type        = string
  default     = ""

}
variable "ms_park_res_valid_sources" {
  description = "incr job starter enablement"
  type        = string
  default     = ""

}

#######################################################
#Variables for Open Search
#######################################################

variable "opensearch_cluster_size" {
  description = "select the cluster size required(small, medium,large)"
  type        = string
}

variable "security_options_enabled" { type = bool }
variable "volume_type" {
  type = string
}
# variable "throughput" {
#   type = number
# }

# variable "opensearch_iops" {
#   default = 3000
#   type    = number
# }
variable "ebs_enabled" {
  type = bool
}
# variable "ebs_volume_size" {
#   type = number
# }

# variable "instance_type" { type = string }
# variable "instance_count" { type = number }
variable "dedicated_master_enabled" {
  type    = bool
  default = true
}
variable "dedicated_master_count" {
  type    = number
  default = 3
}
# variable "dedicated_master_type" {
#   type    = string
#   default = "m6g.large.search"
# }
variable "zone_awareness_enabled" {
  type    = bool
  default = true
}
variable "engine_version" {
  type = string
}
variable "api_gateway_private_certificate" {
  type = string
}
variable "api_gateway_public_certificate" {
  type = string
}
variable "private_dns" {
  type = string
}
variable "public_dns" {
  type = string
}
variable "neptune_batch_size" {
  type = number

}

variable "glue_table_name" {
  description = "Glue table name for common schema"
  type        = string
  default     = "cmmn_schema_tbl"
}

variable "incremental_data_sources_enabled" {
  description = "A list of source names"
  type        = string
  default     = "ALL"
}

variable "max_items" {
  description = "max_items"
  type        = number
  default     = 150
}

variable "max_records" {
  description = "max_records"
  type        = number
  default     = 50000
}
variable "expire_in_days" {
  description = "ddb item expire in days for main-job-complete"
  type        = number
  default     = 90
}

variable "api_path" {
  description = "api path"
  type        = string
  default     = "uxid"
}

variable "api_deployment_stage" {
  description = "api path"
  type        = string
}

variable "neptune_max_bulk_rows" {
  description = "Max number of rows in a single bulk file for neptune bulk load"
  type        = number
  default     = 1000000
}

variable "neptune_is_bulk_compressed" {
  description = "Should the neptune bulk load files be gzip compressed?"
  type        = bool
  default     = true
}
variable "kafka_prefix" {
  description = "kafka source"
  type        = string
  default     = "kafka_sources"
}
variable "batch_prefix" {
  description = "batch sources"
  type        = string
  default     = "batch_sources"
}
variable "debug_output_s3" {
  description = "debug output temp s3"
  type        = string
  default     = "false"
}

variable "ctrl_incr_key" {
  description = "key of incremental job in control table"
  type        = string
  default     = "stop-main-incremental-job-after-current-batch"
}
variable "ctrl_incr_value" {
  description = "value of incremental job in control table"
  type        = string
  default     = "true"
}
variable "ctrl_neptune_key" {
  description = "key of neptune job in control table"
  type        = string
  default     = "stop-main-neptune-job-after-current-batch"
}
variable "ctrl_neptune_value" {
  description = "value of neptune job in control table"
  type        = string
  default     = "true"
}



variable "incr_id_res_job_glue_version" {
  description = "incr id res job glue version"
  type        = string
  default     = "4.0"
}

variable "large_source_etl_job_glue_version" {
  description = "large_source_etl_job_glue_version"
  type        = string
  default     = "4.0"
}
variable "historical_step_1_job_glue_version" {
  description = "historical step 1 job glue version"
  type        = string
  default     = "4.0"
}
variable "historical_step_2_job_glue_version" {
  description = "historical step 2 job glue version"
  type        = string
  default     = "4.0"
}

variable "historical_step_3_job_glue_version" {
  description = "historical step 3 job glue version"
  type        = string
  default     = "4.0"
}

variable "historical_step_4_job_glue_version" {
  description = "historical step 4 job glue version"
  type        = string
  default     = "4.0"
}
variable "neptune_loader_job_glue_version" {
  description = "neptune loader job glue version"
  type        = string
  default     = "4.0"
}
variable "s3_move_job_glue_version" {
  description = "s3 copy job glue version"
  type        = string
  default     = "4.0"
}
variable "s3_move_job_large_src_concurrency" {
  description = "s3 copy job limit concurrency for large source etl job"
  type        = string
  default     = "25"
}

variable "min_instance_metadata_version" {
  description = "minimum instance metadata service version"
  type        = string
  default     = "2"
}

variable "admin_min_instance_metadata_version" {
  description = "minimum instance metadata service version for admin  notebook"
  type        = string
  default     = "2"
}
variable "read_only_min_instance_metadata_version" {
  description = "minimum instance metadata service version for readonly notebook"
  type        = string
  default     = "2"
}

variable "sagemaker_notebook_type" {
  type = string
}
variable "admin_sagemaker_notebook_type" {
  type = string
}
variable "read_only_sagemaker_notebook_type" {
  type = string
}

variable "incremental_id_res_circuit_breaker_ratio" {
  type    = number
  default = 25
}

variable "incremental_id_res_circuit_breaker_min_records" {
  type    = number
  default = 10000
}

variable "reverse_append_preflight_enabled" {
  description = "When true, incr job runs email cache + optional Snowflake fill before id resolution."
  type        = string
  default     = "false"
}

variable "snowflake_jdbc_secret_arn" {
  description = "Secrets Manager ARN for Snowflake JDBC (JSON: account, user, password, warehouse, database, schema). Empty passes DISABLED."
  type        = string
  default     = ""
}

variable "snowflake_enrichment_table_fqn" {
  description = "Snowflake table FQN for lookup, e.g. MY_DB.MY_SCHEMA.MY_TABLE"
  type        = string
  default     = "NOT_CONFIGURED"
}

variable "snowflake_email_column" {
  type    = string
  default = "EMAIL"
}

