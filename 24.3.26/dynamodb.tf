resource "aws_dynamodb_table" "file_status_table" {
  name         = join("-", ["${var.name_prefix}", "idres-files-status-table", "${var.environment}"])
  billing_mode = "PAY_PER_REQUEST" # Use "PAY_PER_REQUEST" for on-demand mode


  hash_key  = "Status"   # Partition key
  range_key = "FileName" # Sort key (optional)

  attribute {
    name = "Status"
    type = "S" # S for String, N for Number, B for Binary
  }

  attribute {
    name = "FileName"
    type = "S" # S for String, N for Number, B for Binary
  }
  attribute {
    name = "Source"
    type = "S" # S for String, N for Number, B for Binary
  }

  global_secondary_index {
    name            = "ReadyToProcessIndex"
    hash_key        = "Source"
    range_key       = "FileName"
    projection_type = "ALL"
  }
  point_in_time_recovery {
    enabled = true

  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.ddb_kms_key
  }
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
  provider = aws.aws-no-defaults
}

#####################################################
###Dynamo DB to store recent glue triggers
#####################################################

resource "aws_dynamodb_table" "glue_invoc_table" {
  name         = join("-", ["${var.name_prefix}", "glue-invocation-table", "${var.environment}"])
  billing_mode = "PAY_PER_REQUEST" # Use "PAY_PER_REQUEST" for on-demand mode
  hash_key     = "hashId"          # Partition key

  attribute {
    name = "hashId"
    type = "S" # S for String, N for Number, B for Binary
  }

  point_in_time_recovery {
    enabled = true

  }
  server_side_encryption {
    enabled     = true
    kms_key_arn = var.ddb_kms_key
  }
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
  provider = aws.aws-no-defaults
}


#####################################################
## Dynamo DB table for neptune tracking table
#####################################################
resource "aws_dynamodb_table" "neptune_tracking_table" {
  name         = join("-", ["${var.name_prefix}", "neptune-status-table", "${var.environment}"])
  billing_mode = "PAY_PER_REQUEST" # Use "PAY_PER_REQUEST" for on-demand mode
  hash_key     = "Status"          # Partition key
  range_key    = "IdResJobId"      # Sort key (optional)

  attribute {
    name = "Status"
    type = "S" # S for String, N for Number, B for Binary
  }

  attribute {
    name = "IdResJobId"
    type = "S" # S for String, N for Number, B for Binary
  }
  attribute {
    name = "Source"
    type = "S" # S for String, N for Number, B for Binary
  }

  global_secondary_index {
    name            = "ReadyToProcessIndex"
    hash_key        = "Source"
    range_key       = "IdResJobId"
    projection_type = "ALL"
  }
  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.ddb_kms_key
  }
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
  provider = aws.aws-no-defaults
}

#####################################################
## Dynamo DB table for incremental control table
#####################################################
resource "aws_dynamodb_table" "incr_control_table" {
  name         = join("-", ["${var.name_prefix}", "incremental-control-table", "${var.environment}"])
  billing_mode = "PAY_PER_REQUEST" # Use "PAY_PER_REQUEST" for on-demand mode
  hash_key     = "JobName"         # Partition key
  #range_key    = "Status"           # Sort key (optional)


  attribute {
    name = "JobName"
    type = "S"
  }

  # attribute {
  #   name = "Status"
  #   type = "S"
  # }

  global_secondary_index {
    name     = "ReadyToProcessIndex"
    hash_key = "JobName"
    #range_key       = "Status"
    projection_type = "ALL"
  }
  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.ddb_kms_key
  }
  provider = aws.aws-no-defaults
}

resource "aws_dynamodb_table_item" "incr_job_status_item" {
  table_name = aws_dynamodb_table.incr_control_table.name
  hash_key   = aws_dynamodb_table.incr_control_table.hash_key
  item       = <<ITEM
{

  "JobName": {"S": "${var.ctrl_incr_key}"},
  "Status": {"S": "${var.ctrl_incr_value}"}
}
ITEM
}

resource "aws_dynamodb_table_item" "neptune_job_status_item" {
  table_name = aws_dynamodb_table.incr_control_table.name
  hash_key   = aws_dynamodb_table.incr_control_table.hash_key
  item       = <<ITEM
{
  "JobName": {"S": "${var.ctrl_neptune_key}"},
  "Status": {"S": "${var.ctrl_neptune_value}"}
}
ITEM
}

#####################################################
###Dynamo DB to store blocked list table
#####################################################

resource "aws_dynamodb_table" "blocklist_table_name" {
  name         = join("-", ["${var.name_prefix}", "blocklist-values", "${var.environment}"])
  billing_mode = "PAY_PER_REQUEST" # Use "PAY_PER_REQUEST" for on-demand mode


  hash_key  = "BlockType"   # Partition key
  range_key = "BlockValue"  # Sort key

  attribute {
    name = "BlockType"
    type = "S" # S for String, N for Number, B for Binary
  }

  attribute {
    name = "BlockValue"
    type = "S" # S for String, N for Number, B for Binary
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.ddb_kms_key
  }
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
  provider = aws.aws-no-defaults
}
resource "aws_dynamodb_table" "reverse_append" {
  name         = join("-", ["${var.name_prefix}", "reverse-append-test", "${var.environment}"])
  billing_mode = "PAY_PER_REQUEST" # Use "PAY_PER_REQUEST" for on-demand mode


  hash_key  = "email_hash"   # Partition key

  attribute {
    name = "email_hash"
    type = "S" # S for String, N for Number, B for Binary
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.ddb_kms_key
  }
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
  provider = aws.aws-no-defaults

  tags = {
    "upt:pci" = "true"
    "udx:dataclassification" = "internal"
    "upt:environment" = "development"
  }
}

resource "aws_dynamodb_table" "udx_address_store_table" {
  name         = join("-", ["${var.name_prefix}", "udx-adddress-store", "${var.environment}"])
  billing_mode = "PAY_PER_REQUEST" # Use "PAY_PER_REQUEST" for on-demand mode


  hash_key  = "original_address"   # Partition key

  attribute {
    name = "original_address"
    type = "S" # S for String, N for Number, B for Binary
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.ddb_kms_key
  }
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
  provider = aws.aws-no-defaults

  tags = {
    "upt:pci" = "true"
    "udx:dataclassification" = "internal"
    "upt:environment" = "development"
  }
}

resource "aws_dynamodb_table" "reverse_append_email_cache" {
  name         = join("-", ["${var.name_prefix}", "reverse-append-email-cache", "${var.environment}"])
  billing_mode = "PAY_PER_REQUEST"

  hash_key = "email_key"

  attribute {
    name = "email_key"
    type = "S"
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.ddb_kms_key
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  provider = aws.aws-no-defaults

  tags = {
    "upt:pci"                = "true"
    "udx:dataclassification" = "internal"
    "upt:environment"        = "development"
  }
}