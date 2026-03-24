
import sys
from awsglue.utils import getResolvedOptions

# Python type hints
from typing import TypeVar, List, Dict
PandasDataFrame = TypeVar('pandas.core.frame.DataFrame')

opensearch_args = {

    "ES_GOLDEN_DB_INDEX_NAME":  "idstore_1",   #idstore_1 testing in pre-prod for uxid issue
    "ES_GOLDEN_DB_NUMBER_SHARDS": 16, # 16 shards for 240M, 6 shards for 20M
    "ES_GOLDEN_DB_NUMBER_REPLICAS": 2,
    "ES_GOLDEN_DB_MAX_SIMILAR_RECORDS_PER_GROUP": 5, # maximum similar records retrieved from the golden db for each group
    "ES_BATCH_SIZE": 250, # batch this many records together when making API calls to prevents API calls with too many or too few records
    "RECORD_FIELDS": {
        'ml_prefix': {"type": "keyword"},
        'ml_first_name': {"type": "keyword"},
        'ml_middle_name': {"type": "keyword"},
        'ml_last_name': {"type": "keyword"}, 
        'ml_generation_qualifier': {"type": "keyword"},
        'ml_gender': {"type": "keyword"},
        'ml_birth_date': {"type": "keyword"},
        'ml_address_line_1': {"type": "keyword"},
        'ml_address_line_2': {"type": "keyword"},
        'ml_city': {"type": "keyword"},
        'ml_state_code': {"type": "keyword"},
        'ml_postal_code': {"type": "keyword"},
        'ml_country_code': {"type": "keyword"},
        'ml_phone': {"type": "keyword"},
        'ml_email_address': {"type": "keyword"},
        'rules_guest_id': {"type": "keyword"}
    },
    "METADATA_FIELDS": {
        'source_name': {"type": "keyword"},
        'source_customer_id': {"type": "keyword"},
        'uxid': {"type": "keyword"},
        'uxid_confidence': {"type": "float"},
        "past_uxids": {
            "type": "nested",
            "properties": {
                "uxid": {"type": "keyword"},
                "uxid_confidence": {"type": "float"},
                "uxid_last_changed_timestamp": {"type": "date", "format": "strict_date_time"}
            }
        },
        'pass_thru_data': {"type": "keyword"},
        'record_created_timestamp': {"type": "date", "format": "strict_date_time"},
        'record_last_updated_timestamp': {"type": "date", "format": "strict_date_time"},
        'id_res_last_updated_timestamp': {"type": "date", "format": "strict_date_time"},
        'updated_by_incremental_batch_id': {"type": "keyword"},
        'updated_by_incremental_batch_description': {"type": "keyword"}
    }
}

"""
entity_resolution_input_data_mappings map Command Schema fields to ER service input schema
"""
entity_resolution_input_data_mappings = [
    {
        "fieldName": "ml_address_line_1",
        "type": "ADDRESS_STREET1",
        "groupName": "Address",
        "matchKey": "Address"
    },
    {
        "fieldName": "ml_address_line_2",
        "type": "ADDRESS_STREET2",
        "groupName": "Address",
        "matchKey": "Address"
    },
    {
        "fieldName": "ml_birth_date",
        "type": "DATE",
        "matchKey": "Date of birth"
    },
    {
        "fieldName": "ml_city",
        "type": "ADDRESS_CITY",
        "groupName": "Address",
        "matchKey": "Address"
    },
    {
        "fieldName": "ml_country_code",
        "type": "ADDRESS_COUNTRY",
        "groupName": "Address",
        "matchKey": "Address"
    },
    {
        "fieldName": "ml_email_address",
        "type": "EMAIL_ADDRESS",
        "matchKey": "Email address"
    },
    {
        "fieldName": "ml_first_name",
        "type": "NAME_FIRST",
        "groupName": "Name",
        "matchKey": "Name"
    },
    {
        "fieldName": "ml_last_name",
        "type": "NAME_LAST",
        "groupName": "Name",
        "matchKey": "Name"
    },
    {
        "fieldName": "ml_middle_name",
        "type": "NAME_MIDDLE",
        "groupName": "Name",
        "matchKey": "Name"
    },
    {
        "fieldName": "ml_phone",
        "type": "PHONE_NUMBER",
        "matchKey": "Phone"
    },
    {
        "fieldName": "ml_postal_code",
        "type": "ADDRESS_POSTALCODE",
        "groupName": "Address",
        "matchKey": "Address"
    },
    {
        "fieldName": "ml_state_code",
        "type": "ADDRESS_STATE",
        "groupName": "Address",
        "matchKey": "Address"
    },
    {
        "fieldName": "pass_thru_data",
        "type": "STRING"
    },
    {
        "fieldName": "ml_prefix",
        "type": "STRING"
    },
    {
        "fieldName": "ml_generation_qualifier",
        "type": "STRING"
    },
    {
        "fieldName": "ml_gender",
        "type": "STRING"
    },
    {
        "fieldName": "tx_datetime",
        "type": "STRING"
    },
    {
        "fieldName": "record_created_timestamp",
        "type": "STRING"
    },
    {
        "fieldName": "record_last_updated_timestamp",
        "type": "STRING"
    },
    {
        "fieldName": "id_res_last_updated_timestamp",
        "type": "STRING"
    },
    {
        "fieldName": "rules_guest_id",
        "type": "STRING"
    },
    {
        "fieldName": "source_name",
        "type": "STRING"
    },
    {
        "fieldName": "source_customer_id",
        "type": "STRING"
    },
    {
        "fieldName": "batch_record_id",
        "type": "UNIQUE_ID"
    },
    {
        "fieldName": "skip_ml",
        "type": "STRING"
    }
]

def get_job_args_incremental():
    # Glue job input argments
    glue_args = getResolvedOptions(sys.argv,
        ['dydb_table', 'neptune_dydb_table', 'max_items','max_records','ctrl_incr_key','incr_ctrl_table','incremental_data_sources_enabled',
         'incremental_common_schema_data_override','incremental_after_pre_ml_rules_path_override','incremental_er_results_path_override',
         'ES_HOST','ES_IAM_MASTER_USER_ARN','ES_REGION',
         'incremental_data_s3_bucket','incremental_glue_db','incremental_glue_table','temp_s3_bucket','er_input_source_arn','er_service_input_schema_name',
         'er_service_workflow_name','ER_IAM_ROLE',
         'opensearch_index_name_override','expire_in_days',
         'NEPTUNE_HOST','NEPTUNE_PORT','NEPTUNE_BATCH_SIZE','NEPTUNE_BULK_ROLE',
         'debug_output_s3', 'incremental_id_res_circuit_breaker_min_records', 'incremental_id_res_circuit_breaker_ratio', 
         'blocklist_table_name',
         'reverse_append_preflight_enabled', 'reverse_append_cache_table', 'snowflake_jdbc_secret_arn',
         'snowflake_enrichment_table_fqn', 'snowflake_email_column'])

    job_args = {
        'dydb_table': glue_args['dydb_table'],
        'neptune_dydb_table':glue_args['neptune_dydb_table'],
        'max_items': glue_args['max_items'],
        'ctrl_incr_key' : glue_args['ctrl_incr_key'],
        'incr_ctrl_table' : glue_args['incr_ctrl_table'],
        'max_records': glue_args['max_records'],
        'incremental_data_sources_enabled': glue_args['incremental_data_sources_enabled'],
        'incremental_common_schema_data_override': glue_args['incremental_common_schema_data_override'],
        'incremental_after_pre_ml_rules_path_override': glue_args['incremental_after_pre_ml_rules_path_override'],
        'incremental_er_results_path_override': glue_args['incremental_er_results_path_override'],
        'ES_HOST': glue_args['ES_HOST'],
        'ES_IAM_MASTER_USER_ARN': glue_args['ES_IAM_MASTER_USER_ARN'],
        'ES_REGION': glue_args['ES_REGION'],
        'incremental_data_s3_bucket': glue_args['incremental_data_s3_bucket'],
        'incremental_glue_db': glue_args['incremental_glue_db'],
        'incremental_glue_table': glue_args['incremental_glue_table'],
        'temp_s3_bucket': glue_args['temp_s3_bucket'],
        'er_input_source_arn': glue_args['er_input_source_arn'],
        'er_service_input_schema_name': glue_args['er_service_input_schema_name'],
        'er_service_workflow_name': glue_args['er_service_workflow_name'],
        'opensearch_index_name_override': glue_args['opensearch_index_name_override'],
        'expire_in_days': glue_args['expire_in_days'],
        'ER_IAM_ROLE': glue_args["ER_IAM_ROLE"],
        'NEPTUNE_HOST': glue_args['NEPTUNE_HOST'],
        'NEPTUNE_PORT': glue_args['NEPTUNE_PORT'],
        'NEPTUNE_BATCH_SIZE': glue_args['NEPTUNE_BATCH_SIZE'],
        'NEPTUNE_BULK_ROLE': glue_args['NEPTUNE_BULK_ROLE'],
        'debug_output_s3': glue_args['debug_output_s3'],
        'incremental_id_res_circuit_breaker_min_records': glue_args['incremental_id_res_circuit_breaker_min_records'],
        'incremental_id_res_circuit_breaker_ratio': glue_args['incremental_id_res_circuit_breaker_ratio'],
        'blocklist_table_name':glue_args['blocklist_table_name'],
        'reverse_append_preflight_enabled': glue_args['reverse_append_preflight_enabled'],
        'reverse_append_cache_table': glue_args['reverse_append_cache_table'],
        'snowflake_jdbc_secret_arn': glue_args['snowflake_jdbc_secret_arn'],
        'snowflake_enrichment_table_fqn': glue_args['snowflake_enrichment_table_fqn'],
        'snowflake_email_column': glue_args['snowflake_email_column'],
    }
    
    return job_args

def get_job_args_incremental_neptune():
    args: list = ['expire_in_days', 'dydb_neptune_file_status_table',
         'NEPTUNE_HOST','NEPTUNE_PORT','NEPTUNE_BATCH_SIZE','NEPTUNE_BULK_ROLE','ctrl_neptune_key','incr_ctrl_table'
    ]
    
    glue_args = getResolvedOptions(sys.argv, args)
    
    job_args = {}
    for x in args:
        job_args[x] = glue_args[x]
    
    return job_args

def s3_copy_job_args():
    args: list = ['source_folder', 'destination_folder', 'large_src_job_name', 'max_job_concurrency', "source_exclude_comma_delimited_list"]
    glue_args = getResolvedOptions(sys.argv, args)
    job_args = {}
    for x in args:
        job_args[x] = glue_args[x]
    
    return job_args


def get_job_args_historical():
    # Glue job input argments
    glue_args = getResolvedOptions(sys.argv, ['JOB_NAME',
        'historical_data_s3_path', 'historical_data_s3_bucket', 'historical_data_s3_format',
        'historical_glue_db', 'historical_glue_table',
        'er_service_input_schema_name', 'er_service_workflow_name', 'er_input_source_arn',
        'opensearch_index_name_override', 'opensearch_historical_writer_partitions_count',
        'id_res_job_id', 'historical_job_er_service_job_id',
        'temp_s3_bucket',
        'SNOWFLAKE_EXPORT_S3_INPUT_PATH',
        'ES_HOST', 'ES_IAM_MASTER_USER_ARN', 'ES_REGION',
        'ER_IAM_ROLE', 'NEPTUNE_HOST', 'NEPTUNE_PORT', 'NEPTUNE_BULK_ROLE', 'NEPTUNE_MAX_BULK_ROWS', 'NEPTUNE_IS_BULK_COMPRESSED',
        'debug_output_s3', 'blocklist_table_name']
    )

    job_args = {
        'job_name': glue_args['JOB_NAME'],
        'historical_data_s3_path': glue_args['historical_data_s3_path'],
        'historical_data_s3_bucket': glue_args['historical_data_s3_bucket'],
        'historical_data_s3_format': glue_args['historical_data_s3_format'],
        'historical_glue_db': glue_args['historical_glue_db'],
        'historical_glue_table': glue_args['historical_glue_table'],
        'er_service_input_schema_name': glue_args['er_service_input_schema_name'],
        'er_service_workflow_name': glue_args['er_service_workflow_name'],
        'er_input_source_arn': glue_args['er_input_source_arn'],
        'opensearch_index_name_override': glue_args['opensearch_index_name_override'],
        'opensearch_historical_writer_partitions_count': glue_args['opensearch_historical_writer_partitions_count'],
        'id_res_job_id': glue_args['id_res_job_id'],
        'historical_job_er_service_job_id': glue_args['historical_job_er_service_job_id'],
        'temp_s3_bucket': glue_args['temp_s3_bucket'],
        'SNOWFLAKE_EXPORT_S3_INPUT_PATH': glue_args['SNOWFLAKE_EXPORT_S3_INPUT_PATH'],
        'ES_HOST': glue_args['ES_HOST'],
        'ES_IAM_MASTER_USER_ARN': glue_args['ES_IAM_MASTER_USER_ARN'],
        'ES_REGION': glue_args['ES_REGION'],
        'ER_IAM_ROLE': glue_args['ER_IAM_ROLE'],
        'NEPTUNE_HOST': glue_args['NEPTUNE_HOST'],
        'NEPTUNE_PORT': glue_args['NEPTUNE_PORT'],
        'NEPTUNE_BULK_ROLE': glue_args['NEPTUNE_BULK_ROLE'],
        'NEPTUNE_MAX_BULK_ROWS': glue_args['NEPTUNE_MAX_BULK_ROWS'],
        'NEPTUNE_IS_BULK_COMPRESSED': glue_args['NEPTUNE_IS_BULK_COMPRESSED'],
        'debug_output_s3': glue_args['debug_output_s3'],
        'blocklist_table_name':glue_args['blocklist_table_name']
    }
    
    return job_args

def get_opensearch_args(job_args: dict):
    if job_args['opensearch_index_name_override'].lower() != 'not_overridden_use_default':
        print('ES_GOLDEN_DB_INDEX_NAME override is set to:', job_args['opensearch_index_name_override'])
        opensearch_args['ES_GOLDEN_DB_INDEX_NAME'] = job_args['opensearch_index_name_override']
    return opensearch_args

def get_common_schema_columns():
    """
    Common Schema column names.
    Note for historical data: please see the function get_raw_historical_column_name() in historical_data.py because
    some of the column names appear as a different name in the raw historical data.
    """
    
    # Common schema columns
    metadata_cols = [
        "source_name",
        "source_customer_id",
        "record_created_timestamp",
        "record_last_updated_timestamp",
        "id_res_last_updated_timestamp",
        "uxid",
        "uxid_confidence",
        "pass_thru_data"
    ]

    ml_cols = [        
        "prefix",
        "first_name",
        "middle_name",
        "last_name",
        "generation_qualifier",
        "gender",
        "birth_date",
        "address_line_1",
        "address_line_2",
        "city",
        "state_code",
        "postal_code",
        "country_code",
        "phone",
        "email_address",
    ]

    rules_cols = [
        "guest_id",
    ]

    # snowflake_export_cols = [
    #     "source_name",
    #     "source_customer_id",
    #     "record_created_timestamp",
    #     "record_last_updated_timestamp",
    #     "uxid",
    #     "uxid_confidence"
    # ]

    return {
        'metadata_cols': metadata_cols,
        'ml_cols': ml_cols,
        'rules_cols': rules_cols
        #'snowflake_export_cols': snowflake_export_cols
    }

def get_entity_resolution_input_mappings():
    return entity_resolution_input_data_mappings

def get_entity_resolution_output_mappings(input_mappings: list):
    # Output mappings should be the same fields as the input mappings, so calculate this
    output_mappings = []

    input_mappings = get_entity_resolution_input_mappings()
    for field in input_mappings:    
        output_mappings.append({ "name": field['fieldName'], "hashed": False })
    
    return output_mappings