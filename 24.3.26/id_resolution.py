"""
Contains workflow and logic for id resolution
"""

from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, concat_ws, col, regexp_extract,udf,date_format,to_timestamp, lit, when, trim, lower, from_json, to_json
from pyspark.sql.types import StringType, DoubleType, TimestampType, StructField, StructType, IntegerType, ArrayType
from functools import partial
from datetime import datetime
from dateutil import parser
import pytz
import json
import os
import time
import math
import logging
import boto3

from rules.id_res_rule_manager import IdResRuleManager
from id_opensearch import create_opensearch_client, create_and_update_opensearch_idstore_index, bulk_write_to_opensearch
from id_neptune import writeIncrementalPartitionToNeptune
from id_resolution_exact_matcher import process_exact_match_for_each_partition
from id_resolution_similar_matcher import do_similarity_search, do_similarity_search_extended
from args_and_constants import get_opensearch_args,get_job_args_incremental
from incremental_data import create_or_update_er_input_table
from er_management import create_or_update_schema_mapping_and_workflow
from entity_resolution import start_matching_job
from batch_data import union_bypass_dataframe
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
from pyspark.storagelevel import StorageLevel
from batch_data import dydb_neptune_put_item

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s')
logger = logging.getLogger(__name__)


def calculate_all_sim_field(df_batch):
    """
    Purpose:
    - Calculate the all_sim filed value from row_dict, used for OpenSearch similarity search

    Inputs:
    - df_batch: Spark dataframe
    
    Outputs:
    - df_batch with all_sim field added
    """

    # create "tmp_email_no_domain" column which is the email address without the @ and domain name
    # Note: tested successfully these values: null, empty string, no @ symbol, start with @ and end with @
    df_batch = df_batch.withColumn("tmp_email_no_domain", regexp_extract(col("ml_email_address"), "^([^@]+)@", 1))

    # print('calculate_all_sim_field')

    # create the "all_sim" column which concatenates some of the fields.
    # this "all_sim" column is used for OpenSearch similarity search during id resolution.
    # Note: concat_ws ignores null values so it's safe to use if some columns are null
    df_batch = df_batch.withColumn("all_sim", concat_ws(" ",
        col("ml_first_name"),
        col("ml_middle_name"),
        col("ml_last_name"),
        col("ml_address_line_1"),
        col("ml_address_line_2"),
        col("ml_phone"),
        col("tmp_email_no_domain")
    ))

    return df_batch

@udf(StringType())
def convert_timestamp_udf(dt):
    """
    UDF to convert various timestamp formats to ISO format with error handling
    
    Args:
        dt (str): Input timestamp string including format "2024-10-23:02:54:32.458183"
    
    Returns:
        str: ISO formatted timestamp string, or None if conversion fails
    """
    if not dt:
        return None
        
    try:
        # Handle the specific format "YYYY-mm-dd:hh:mm:ss.ssssss"
        if ':' in dt and len(dt.split(':')) == 4:
            try:
                # Replace single ':' between date and time with space
                dt_formatted = dt.replace(':', ' ', 1)
                parsed_date = datetime.strptime(dt_formatted, '%Y-%m-%d %H:%M:%S.%f')
            except ValueError as e:
                print(f"Specific format parsing failed: {str(e)}")
                # If specific format fails, try general parser
                parsed_date = parser.parse(dt)
        else:
            # For other formats, use dateutil parser
            parsed_date = parser.parse(dt)
        
        # Convert to UTC if timezone is present, otherwise assume UTC
        if parsed_date.tzinfo is None:
            parsed_date = parsed_date.replace(tzinfo=pytz.UTC)
            
        # Format in ISO format with milliseconds
        return parsed_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        
    except Exception:
        return None

def convert_timestamps(df_batch):
    """
    Convert timestamps in the DataFrame to ISO format using UDF
    """
    try:
        initial_count = df_batch.count()
        df_converted = df_batch
        timestamp_columns = [
            "record_created_timestamp",
            "record_last_updated_timestamp"
        ]
        
        df_converted = df_batch
        for column in timestamp_columns:
            # Convert timestamps
            df_converted = df_converted.withColumn(
                f"{column}_temp",                
                convert_timestamp_udf(F.col(column))
            )

            # Drop records where timestamp conversion failed
            invalid_records = df_converted.filter(
                col(f"{column}_temp").isNull() & col(column).isNotNull()
            )
            invalid_count = invalid_records.count()
            
            if invalid_count > 0:
                # Drop invalid records
                df_converted = df_converted.filter(
                    (col(f"{column}_temp").isNotNull()) | (col(column).isNull())
                )

            # Rename column
            df_converted = df_converted.drop(column).withColumnRenamed(
                f"{column}_temp",
                column
            )

        # Calculate and log the total number of dropped records
        final_count = df_converted.count()
        dropped_count = initial_count - final_count
        
        if dropped_count > 0:
            logger.warning(
                f"Dropped {dropped_count} records due to invalid timestamps "
                f"({dropped_count/initial_count*100:.2f}% of total records)"
            )                    
        return df_converted
    except Exception as e:
        error_msg = f"Error converting timestamps: {str(e)}"
        logger.error(error_msg)
        raise


def wait_for_job_completion(client, job_id, workflow_name, max_attempts=12000):
    attempts = 0
    while attempts < max_attempts:
        response = client.get_matching_job(jobId=job_id,workflowName=workflow_name)
        status = response['status']
        if status == 'SUCCEEDED':
            print(f"Job {job_id} completed successfully.")
            return True
        elif status == 'FAILED':
            error_msg = f"Job {job_id} failed with status FAILED."
            print(error_msg)
            raise Exception(error_msg)
        elif status in ['RUNNING', 'QUEUED']:
            print(f"Job {job_id} is still {status}. Waiting...")
            time.sleep(30)  # Wait for 45 sec
            attempts += 1
        else:
            error_msg = f"Job {job_id} failed with unexpected status: {status}"
            print(error_msg)
            raise Exception(error_msg)
    error_msg = f"Job {job_id} did not complete within the expected time (timeout after {max_attempts} attempts)."
    print(error_msg)
    raise Exception(error_msg)


#def write_spark_partition_to_opensearch(partition):
def write_spark_partition_to_opensearch(partition,host,iam_master_user_arn,region,index_name,record_fields,metadata_fields,batch_size):
    opensearch_client = create_opensearch_client(
        host=host,
        iam_master_user_arn=iam_master_user_arn,
        region=region
    )

    #index_to_write_to = opensearch_args['ES_GOLDEN_DB_INDEX_NAME']
    index_to_write_to = index_name
    bulk_operations = []
    record_fields = record_fields
    metadata_fields = metadata_fields
    batch_size = batch_size    

    row_count = 0
    for row in partition:
        row_count += 1

        row_dict = row.asDict(True)

        # fill the es_doc object which is an OpenSearch document using data in Spark row
        es_doc = {}
        original_record = {} # original transformed common schema data, NOT normalized by ER service
        normalized_record = {} # original transformed common schema data, after normalization by ER service

        for column_name, value in row_dict.items():
            #if column_name in opensearch_args['RECORD_FIELDS'] or column_name in opensearch_args['METADATA_FIELDS']:
            if column_name in record_fields or column_name in metadata_fields:
                if column_name.startswith('ml_') or column_name.startswith('rules_'):
                    normalized_record[column_name] = value
                else:
                    es_doc[column_name] = value
            elif column_name.startswith('original_'):
                original_record[column_name[len('original_'):]] = value
        es_doc['all_sim'] = row_dict['all_sim'] # all_sim is used for similarity search
        es_doc['record_original'] = original_record
        es_doc['record_normalized'] = normalized_record

        operation = {
            "update": {
                "_index": index_to_write_to,
                "_id": es_doc['source_name'] + '_' + es_doc['source_customer_id']
            }
        }
        document = {
            "doc": es_doc,
            "doc_as_upsert": True
        }
        bulk_operations.append(operation)
        bulk_operations.append(document)

        # bulk write multiple es_doc objects at one time into OpenSearch
        #if row_count % opensearch_args['ES_BATCH_SIZE'] == 0:
        if row_count % batch_size == 0:
            print(len(bulk_operations), row_count)
            row_count = 0
            #print('bulk_operations', bulk_operations)
            bulk_write_to_opensearch(opensearch_client, bulk_operations)
            bulk_operations = []
    #print('bulk_operations', bulk_operations)
    if bulk_operations:
        bulk_write_to_opensearch(opensearch_client, bulk_operations) # write last partial batch to OpenSearch
    
    opensearch_client.close() # important, close the client for this group because there are many groups

# function to perform bulk update to opensearch
def bulk_timestamp_and_pass_thru_data_update_to_opensearch(partition,host,iam_master_user_arn,region,index_name):
    opensearch_client = create_opensearch_client(
        host=host,
        iam_master_user_arn=iam_master_user_arn,
        region=region
    )
    
    row_count = 0
    bulk_updates=[]
    for row in partition:
        row_count += 1
        row_dict = row.asDict(True)
        es_doc = {
            "_op_type": "update",  
            "_index": index_name,
            "_id": row_dict['source_name'] + '_' + row_dict['source_customer_id'],
            "doc": {
                    "updated_by_incremental_batch_id": row_dict['updated_by_incremental_batch_id'],
                    "updated_by_incremental_batch_description" : row_dict['updated_by_incremental_batch_description'],
                    "record_last_updated_timestamp":  row_dict['record_last_updated_timestamp'],
                    "pass_thru_data":  row_dict['pass_thru_data']
            }
        }
        bulk_updates.append(es_doc)
            
    print(bulk_updates)
    if len(bulk_updates) > 0:
        # Perform the bulk update operation
        success, failed = bulk(opensearch_client, bulk_updates)
        # Print the result of the bulk operation
        print(f"Failed updates: {failed}")
    opensearch_client.close()

def _convert_exact_search_df(df_exact_search_result, past_uxids_schema):
    """
    Exact search returns normalized column values as a JSON string in record_normalized column, and original column values in ml_ and rules_ columns.
    This method converts the ml_ and rules_ columns into original_ml_ and original_rules_ columns, and explodes record_normalized columns as ml_ and rules_ columns.
    The purpose of this method is convert the exact search result dataframe into a dataframe that is compatible with the snowflake_export schema.
    """
    # all columns starts with ml_ and rules_ to be part of json_schema
    ml_columns = [col for col in df_exact_search_result.columns if col.startswith("ml_")]
    rules_columns = [col for col in df_exact_search_result.columns if col.startswith("rules_")]
    formatted_columns = [f"{col} string" for col in ml_columns + rules_columns]
    json_schema = ", ".join(formatted_columns)

    # replace ml_ and rules_ columns names with original_
    for column in df_exact_search_result.columns:
        df_exact_search_result = df_exact_search_result.withColumnRenamed(column, 'original_'+column if column.startswith("ml_") or column.startswith("rules_") else column)

    df_batch_exact_match_renamed = df_exact_search_result.withColumn("x", F.from_json("record_normalized", json_schema)).select("*", "x.*")
    columns_to_keep = [col for col in df_batch_exact_match_renamed.columns if not col.startswith("x")]

    # Select only the columns to keep
    df_batch_exact_match_renamed = df_batch_exact_match_renamed.select(*columns_to_keep)
    
    # convert the past_uxids from string to array column for s3 dataframe
    df_batch_exact_match_renamed = df_batch_exact_match_renamed.withColumn("past_uxids", from_json(col("past_uxids"), past_uxids_schema))

    df_batch_exact_match_renamed = df_batch_exact_match_renamed.drop('record_normalized')

    return df_batch_exact_match_renamed

def perform_id_resolution_before_er(df_batch, spark_context, id_res_job_id: str):
    try:
        job_args = get_job_args_incremental()
        opensearch_args = get_opensearch_args(job_args)

        # partition df_batch to at most size of OpenSearch batch
        df_batch = df_batch.repartition(math.ceil(df_batch.count()/opensearch_args['ES_BATCH_SIZE']))

        intermediate_s3_output_path: str = f"s3://{job_args['incremental_data_s3_bucket']}/intermediate_data/"

        if job_args['debug_output_s3'].strip().lower() == 'true':
            debug_path = os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0000_initial_loaded_superbatch')
            df_batch.write.parquet(debug_path)

        print('Before pre_search_rules size_total: ', df_batch.count())
        # Apply Presearch rules
        print('Running pre-search rules...')
        df_batch = IdResRuleManager("pre_search_rules", id_res_job_id, intermediate_s3_output_path, job_args['debug_output_s3']).applyRulesInOrder(df_batch)
        print('Running pre-search rules...Done')
        print('After pre_search_rules size_total: ', df_batch.count())
        
        # Add the "all_sim" data field needed for OpenSearch similarity Search
        df_batch = calculate_all_sim_field(df_batch=df_batch)

        # Create necessary OpenSearch indexes, if not already created previously
        opensearch_client = create_opensearch_client(
            host=job_args['ES_HOST'],
            iam_master_user_arn=job_args['ES_IAM_MASTER_USER_ARN'],
            region=job_args['ES_REGION']
        )
        print('OpenSearch client:')
        print(opensearch_client)
        create_and_update_opensearch_idstore_index(
            opensearch_client=opensearch_client,
            index_name=opensearch_args['ES_GOLDEN_DB_INDEX_NAME'],
            number_shards=opensearch_args['ES_GOLDEN_DB_NUMBER_SHARDS'],
            number_replicas=opensearch_args['ES_GOLDEN_DB_NUMBER_REPLICAS'],
            record_fields=opensearch_args['RECORD_FIELDS'],
            metadata_fields=opensearch_args['METADATA_FIELDS']
        )

        # convert blanks and 0 length strings data for only string columns to null
        df_string_columns = [c for c, dtype in df_batch.dtypes if dtype == 'string']
        for column in df_string_columns:
            df_batch = df_batch.withColumn(column, when(trim(col(column)) == '', None).otherwise(col(column)))

        # running convert_timestamps on df_batch to ensure timestamps are in isoformat
        df_batch = convert_timestamps(df_batch)

        if job_args['debug_output_s3'].strip().lower() == 'true':
            debug_path = os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0010_after_presearch_rules_and_all_sim')
            df_batch.write.parquet(debug_path)

        print('record count for exact search size_total: ', df_batch.count())

        # code to reparition the data based on number of executors and processors
        num_executors =  int(spark_context.conf.get("spark.executor.instances", "1"))
        cores_per_executor = int(spark_context.conf.get("spark.executor.cores", "1"))
        total_partitions = num_executors * cores_per_executor
        print(f"Number of executors and cores in spark cluster: {num_executors},{cores_per_executor},{total_partitions}")

        # get count of active data node from opensearch cluster
        nodes_info = opensearch_client.nodes.info()
        active_data_nodes = sum(1 for node_id, node_info in nodes_info['nodes'].items() if node_info['roles'] and 'data' in node_info['roles'])
        active_node_ratio = 2/3
        active_nodes_after_standby = math.floor(active_data_nodes * active_node_ratio)
        print(f"Number of active data nodes minus stand-by in cluster: {active_nodes_after_standby}")

        # get cpu count of each data node to set max_concurrent_value
        cpu_on_data_nodes = int(sum(node_info['os']['available_processors'] for node_id, node_info in nodes_info['nodes'].items() 
                            if node_info['roles'] and 'data' in node_info['roles']) / active_data_nodes)
        print(f"Number of cpu counts on each data nodes : {cpu_on_data_nodes}")
        max_concurrent_searches=math.floor((int((cpu_on_data_nodes*3)/2)+1)*0.8)
        print(f"Number of max_concurrent_searches at 80 percent on each data nodes : {max_concurrent_searches}")
        
        # partition the input dataframe for optmimal opensearch msearch
        print("Number of df_batch partitions before:",df_batch.rdd.getNumPartitions())
        df_batch = df_batch.repartition(int(active_nodes_after_standby))
        print("Number of df_batch partitions after:", df_batch.rdd.getNumPartitions())
        
        # exact match search starts from here
        df_batch = df_batch \
            .withColumn("es_search_results_s1", lit(None).cast(StringType())) \
            .withColumn("es_search_results_s2", lit(None).cast(StringType())) \
            .withColumn("es_search_results_s3", lit(None).cast(StringType())) \
            .withColumn("delete_flag", lit("N").cast(StringType())) \
            .withColumn("uxid", lit(None).cast(StringType())) \
            .withColumn("uxid_confidence", lit(1.0).cast(DoubleType())) \
            .withColumn("id_res_last_updated_timestamp", lit(None).cast(StringType())) \
            .withColumn("processing_scenario", lit(0).cast(IntegerType())) \
            .withColumn("record_normalized", lit(None).cast(StringType())) \
            .withColumn("past_uxids", lit(None).cast(StringType())) \
            .withColumn("updated_by_incremental_batch_id", lit(id_res_job_id).cast(StringType())) \
            .withColumn("updated_by_incremental_batch_description", lit(None).cast(StringType())) 

        schema_df_batch = df_batch.schema

        if job_args['debug_output_s3'].strip().lower() == 'true':
            path_exact_df: str = os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0020_before_exact_search')
            df_batch.write.parquet(path_exact_df)
        
        # calling mapInPandas to perform exact match processing_scenario
        process_exact_match_for_each_partition_with_jobargs = partial(process_exact_match_for_each_partition, job_args=job_args, max_concurrent_searches=max_concurrent_searches, id_res_job_id=id_res_job_id)
        df_batch_exact_match = df_batch.mapInPandas(process_exact_match_for_each_partition_with_jobargs, schema=schema_df_batch).persist(StorageLevel.DISK_ONLY)
        
        if job_args['debug_output_s3'].strip().lower() == 'true':
            df_batch_exact_match.write.parquet(os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0030_df_batch_exact_match_results'))
            

        print("count of records with processing_scenario 1:",df_batch_exact_match.select().where((df_batch_exact_match.processing_scenario==1)).count())
        print("count of records with processing_scenario 2:",df_batch_exact_match.select().where((df_batch_exact_match.processing_scenario==2)).count())
        print("count of records with processing_scenario 3:",df_batch_exact_match.select().where((df_batch_exact_match.processing_scenario==3)).count())
        print("count of records with processing_scenario 4:",df_batch_exact_match.select().where((df_batch_exact_match.processing_scenario==4)).count())

        # drop columns which are not needed further
        df_batch_exact_match=df_batch_exact_match.drop("es_search_results_s1", "es_search_results_s2","es_search_results_s3")
        
        # filter spark dataframe based on processing_scenario as separate action needs to be taken
        df_batch_exact_match_s1=df_batch_exact_match.filter("processing_scenario==1 and delete_flag=='N'") 
        df_batch_exact_match_s2=df_batch_exact_match.filter("(processing_scenario==2 or processing_scenario==4) and delete_flag=='N'") 
        df_batch_exact_match_s3=df_batch_exact_match.filter("processing_scenario==3")

        print('(Output of exact search) df_batch_exact_match.count()', df_batch_exact_match.count())

        if job_args['debug_output_s3'].strip().lower() == 'true':
            delete_flag_yes_path: str = os.path.join(intermediate_s3_output_path, 'dropped_records', id_res_job_id, '0040_delete-flag-yes-dropped-records')
            df_batch_exact_match.filter('delete_flag=="Y"').write.parquet(delete_flag_yes_path)

        # update batch descr column values
        df_batch_exact_match_s1 = df_batch_exact_match_s1.withColumn("updated_by_incremental_batch_description", lit("incremental_exact_s1"))
        df_batch_exact_match_s3 = df_batch_exact_match_s3.withColumn("updated_by_incremental_batch_description", lit("incremental_exact_s3"))
        df_batch_exact_match_s2 = df_batch_exact_match_s2.withColumn("updated_by_incremental_batch_description", when(df_batch_exact_match_s2.processing_scenario==2,"incremental_exact_s2").otherwise("incremental_exact_s4"))
        
        # process processing_scenario 1 records
        if df_batch_exact_match_s1.count() > 0:
            # write record_last_updated_timestamp into OpenSearch
            df_batch_exact_match_s1.foreachPartition(lambda partition: bulk_timestamp_and_pass_thru_data_update_to_opensearch(                    
                partition,
                host = job_args['ES_HOST'],
                iam_master_user_arn = job_args['ES_IAM_MASTER_USER_ARN'],
                region = job_args['ES_REGION'],
                index_name = opensearch_args['ES_GOLDEN_DB_INDEX_NAME']
            ))
            print("processing_scenario 1 records are updated into opensearch")

        # change data type of past_uxid from string to array type
        past_uxids_schema = ArrayType(
            StructType([
                StructField("uxid", StringType(), True),
                StructField("uxid_confidence", DoubleType(), True),
                StructField("uxid_last_changed_timestamp", StringType(), True)
            ])
        )

        df_batch_s1_output = _convert_exact_search_df(df_batch_exact_match_s1, past_uxids_schema).persist(StorageLevel.DISK_ONLY)
        df_batch_s1_output = df_batch_s1_output.drop("processing_scenario","record_normalized","delete_flag")

        # drop addtional columns from processing_scenario 2 & 4 records from dataframe as 2 & 4 records to participate in ER resolution via similarty search
        df_batch_exact_match_s2 = df_batch_exact_match_s2.drop("processing_scenario","record_normalized","delete_flag")

        print('final number of records output going to similarty search part of processing_scenario 2 & 4 are: ', df_batch_exact_match_s2.count())
        print('printing schema of processing_scenario 2 & 4 dataframe: ', df_batch_exact_match_s2.printSchema())
        
        df_batch_exact_match_s2 = df_batch_exact_match_s2.withColumn("past_uxids", from_json(col("past_uxids"), past_uxids_schema))
        print('printing schema of processing_scenario 2 & 4 after past_uxids data type change:', df_batch_exact_match_s2.printSchema())

        # actions are taken for processing_scenario 3 for renaming the ml and rules fields to original_
        # also adding fileds from opensearch record_normalized to spark dataframe (ml_ and rules_) and insert the record into opensearch
        df_count_exact_match_s3=df_batch_exact_match_s3.count()
        #print('number of records as part of processing_scenario 3: ', df_count_exact_match_s3)

        if job_args['debug_output_s3'].strip().lower() == 'true':
            temp_data_exact_match_s2_path: str = os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0040_exact-search-s2-s4-results')
            df_batch_exact_match_s2.write.parquet(temp_data_exact_match_s2_path)
            temp_data_exact_match_s1_path: str = os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0040_exact-search-s1-results')
            df_batch_exact_match_s1.write.parquet(temp_data_exact_match_s1_path)
        
        df_batch_exact_match_s3 = df_batch_exact_match_s3.persist(StorageLevel.DISK_ONLY)
        df_batch_s3_output = _convert_exact_search_df(df_batch_exact_match_s3, past_uxids_schema).persist(StorageLevel.DISK_ONLY)
        df_batch_s3_output = df_batch_s3_output.drop("processing_scenario","record_normalized","delete_flag")

        if df_count_exact_match_s3 > 0:        
            print('printing schema of processing_scenario 3 df before writing into opensearch: ', df_batch_s3_output.printSchema())
            
            if job_args['debug_output_s3'].strip().lower() == 'true':
                temp_data_exact_match_s3_path: str = os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0040_exact-search-s3-results')
                df_batch_s3_output.write.parquet(temp_data_exact_match_s3_path)

            print('inserting into opensearch for processing_scenario 3')
            # inserting record into opensearch for processing_scenario 3. currently, this processing_scenario could bring mutiple record from opensearc
            # but we are picking up only one record randomly and using it to insert into opensearch
            df_batch_s3_output.foreachPartition(lambda partition: write_spark_partition_to_opensearch(                    
                partition,
                host = job_args['ES_HOST'],
                iam_master_user_arn = job_args['ES_IAM_MASTER_USER_ARN'],
                region = job_args['ES_REGION'],
                index_name = opensearch_args['ES_GOLDEN_DB_INDEX_NAME'],
                record_fields = opensearch_args['RECORD_FIELDS'],
                metadata_fields = opensearch_args['METADATA_FIELDS'],
                batch_size = opensearch_args['ES_BATCH_SIZE']
            ))
            print("processing_scenario 3 records are inserted into opensearch")
        
        # final dataframe output going from exact search to similarity search
        df_batch = df_batch_exact_match_s2.persist(StorageLevel.DISK_ONLY)

        if job_args['debug_output_s3'].strip().lower() == 'true':
            path_incremental_after_exact_search: str = os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0050_incremental_after_exact_search')
            df_batch.write.parquet(path_incremental_after_exact_search)
        
        # scenario 1 and scenario 3 outputs should be returned separately, because they don't go through id res rules nor ER service
        df_batch_output_bypass_id_res = df_batch_s1_output.union(df_batch_s3_output)
        # Apply Pre-ML Rules to df_batch_output_bypass_id_res. This dataframe won't go through the rest of id resolution, but applying pre-ml to standardize some outputs such as skip_ml
        df_batch_output_bypass_id_res = IdResRuleManager("pre_ml_rules", id_res_job_id+"_bypass_dataframe", intermediate_s3_output_path, job_args['debug_output_s3']).applyRulesInOrder(df_batch_output_bypass_id_res).persist(StorageLevel.DISK_ONLY)
        df_batch_output_bypass_id_res = df_batch_output_bypass_id_res.withColumn('skip_ml', df_batch_output_bypass_id_res['skip_ml'].cast(StringType()))    

        if job_args['debug_output_s3'].strip().lower() == 'true':
            df_batch_output_bypass_id_res.write.parquet(os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0060_df_batch_output_bypass_id_res'))

        #Apply similarity search
        print("Number of df_batch partitions before sim search:", df_batch.rdd.getNumPartitions())
        
        df_batch_similar_records = do_similarity_search(df_batch, max_concurrent_searches=max_concurrent_searches, id_res_job_id=id_res_job_id).persist(StorageLevel.DISK_ONLY)
        
        print('(Output of sim search) df_batch_similar_records.count()', df_batch_similar_records.count())

        df_email_addresses_input = df_batch.filter('ml_email_address is not null').select('ml_email_address').distinct()
        df_guest_id_input = df_batch.filter('rules_guest_id is not null').select('rules_guest_id').distinct()
        df_uxid_input = df_batch.filter('uxid is not null').select('uxid').unionByName(df_batch_similar_records.filter('uxid is not null').select('uxid'))

        print("Number of df_batch_similar_records partitions before sim search extended:",df_batch_similar_records.rdd.getNumPartitions())
        df_email_addresses_results, df_guest_ids_results, df_uxids_results = do_similarity_search_extended(df_email_addresses_input, df_guest_id_input, df_uxid_input, df_batch_similar_records.schema, max_concurrent_searches=max_concurrent_searches, id_res_job_id=id_res_job_id)

        print('df_email_addresses_results.count()', df_email_addresses_results.count())
        print('df_guest_ids_results.count()', df_guest_ids_results.count())
        print('df_uxids_results.count()', df_uxids_results.count())

        if job_args['debug_output_s3'].strip().lower() == 'true':
            df_batch_similar_records.write.parquet(os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0070_incremental_df_batch_similar_records'))
        
        df_combined_extended_similar_records = df_batch_similar_records.unionByName(df_email_addresses_results, allowMissingColumns=True).unionByName(df_guest_ids_results, allowMissingColumns=True).unionByName(df_uxids_results, allowMissingColumns=True)
        print('df_combined_extended_similar_records count: ', df_combined_extended_similar_records.count())

        if job_args['debug_output_s3'].strip().lower() == 'true':
            df_combined_extended_similar_records.write.parquet(os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0080_incremental_df_combined_extended_similar_records'))
        
        df_input_and_all_similar_records = df_batch.unionByName(df_combined_extended_similar_records, allowMissingColumns=True).persist(StorageLevel.DISK_ONLY)
        print('input records + regular and extended similar records count: ', df_input_and_all_similar_records.count())

        # it's often the case that similarity search returns the same record multiple times, so, removing these
        df_input_and_all_similar_records = df_input_and_all_similar_records.distinct().persist(StorageLevel.DISK_ONLY)
        print('input records + regular and extended distinct similar records count: ', df_input_and_all_similar_records.count())

        if job_args['debug_output_s3'].strip().lower() == 'true':
            path_incremental_after_similarity_search: str = os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0090_incremental_after_similarity_search')
            df_input_and_all_similar_records.write.parquet(path_incremental_after_similarity_search)

        # Apply Pre-ML Rules
        print('Running Pre-ML Rules...')
        df_batch = IdResRuleManager("pre_ml_rules", id_res_job_id, intermediate_s3_output_path, job_args['debug_output_s3']).applyRulesInOrder(df_input_and_all_similar_records).persist(StorageLevel.DISK_ONLY)
        print('Running Pre-ML Rules...Done')
        print('after pre_ml_rules size_total: ', df_batch.count())

        if job_args['debug_output_s3'].strip().lower() == 'true':
            path_incremental_after_pre_ml_rules: str = os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0100_incremental_after_pre_ml_rules')
            df_batch.write.parquet(path_incremental_after_pre_ml_rules)

        opensearch_client.close()
        
        return df_batch, df_batch_output_bypass_id_res
    except Exception as e:
        print('perform_id_resolution_before_er():', str(e))
        raise
    
def perform_id_resolution_er_and_after(df_batch, df_batch_output_bypass_id_res, spark_context, id_res_job_id: str, batch_candidate_data_count: int):
    try:
        job_args = get_job_args_incremental()
        opensearch_args = get_opensearch_args(job_args)

        intermediate_s3_output_path: str = f"s3://{job_args['incremental_data_s3_bucket']}/intermediate_data/"

        incremental_after_pre_ml_rules_path_override: str = job_args['incremental_after_pre_ml_rules_path_override']
        if incremental_after_pre_ml_rules_path_override.strip().lower() != 'not_overridden_use_default':
            print(f'incremental_after_pre_ml_rules_path_override is set to {incremental_after_pre_ml_rules_path_override}. Loading df_batch as this dataframe.')
            df_batch = spark_context.read.parquet(incremental_after_pre_ml_rules_path_override)
        
        # Perform ML Resolution (ER Service)
        #incremental_common_schema_transformed_data_s3_path: str = f's3://{job_args["incremental_data_s3_bucket"]}/er_input/{id_res_job_id}/'
        incremental_common_schema_transformed_data_s3_path_skip_ml: str = f's3://{job_args["incremental_data_s3_bucket"]}/transformed_incoming/{id_res_job_id}/skip_ml=true/'
        incremental_common_schema_transformed_data_s3_path_do_ml: str = f's3://{job_args["incremental_data_s3_bucket"]}/transformed_incoming/{id_res_job_id}/skip_ml=false/'
        athena_tmp_results_path: str = f's3://{job_args["temp_s3_bucket"]}/tmp_athena_results/{id_res_job_id}/'

        df_ids_skip_ml = df_batch.select('batch_record_id', 'past_uxids')

        # write the skip_ml = True records
        print('incremental_common_schema_transformed_data_s3_path_skip_ml', incremental_common_schema_transformed_data_s3_path_skip_ml)
        df_original_skipped_ml = df_batch.filter(df_batch.skip_ml == True)
        df_original_skipped_ml.write.parquet(incremental_common_schema_transformed_data_s3_path_skip_ml)    
        df_original_skipped_ml = df_original_skipped_ml.withColumn('skip_ml', df_original_skipped_ml['skip_ml'].cast(StringType()))    

        # write the skip_ml = False records
        print('incremental_common_schema_transformed_data_s3_path_do_ml', incremental_common_schema_transformed_data_s3_path_do_ml)
        df_main_do_ml = df_batch.filter(df_batch.skip_ml == False).drop('past_uxids')
        df_main_do_ml.write.parquet(incremental_common_schema_transformed_data_s3_path_do_ml)

        # circuit breaker check for ER service counts
        incremental_id_res_circuit_breaker_min_records = int(job_args['incremental_id_res_circuit_breaker_min_records'])
        incremental_id_res_circuit_breaker_ratio = float(job_args['incremental_id_res_circuit_breaker_ratio'])
        er_service_record_count: int = df_main_do_ml.count()

        if batch_candidate_data_count == 0:
            print('perform_id_resolution_er_and_after(): circuit breaker check not needed because batch_candidate_data_count=0.')
        else:
            er_service_record_ratio: float = float(er_service_record_count)/float(batch_candidate_data_count)
            if er_service_record_count >= incremental_id_res_circuit_breaker_min_records and er_service_record_ratio >= incremental_id_res_circuit_breaker_ratio:
                msg = f'perform_id_resolution_er_and_after(): circuit breaker check failed. Stopping processing. er_service_record_ratio=er_service_record_count/batch_candidate_data_count={er_service_record_count}/{batch_candidate_data_count}={er_service_record_ratio}, er_service_record_count={er_service_record_count}, incremental_id_res_circuit_breaker_min_records={incremental_id_res_circuit_breaker_min_records}, incremental_id_res_circuit_breaker_ratio={incremental_id_res_circuit_breaker_ratio}'
                raise Exception(msg)
            else:
                print(f'perform_id_resolution_er_and_after(): circuit breaker check success. er_service_record_ratio=er_service_record_count/batch_candidate_data_count={er_service_record_count}/{batch_candidate_data_count}={er_service_record_ratio}, er_service_record_count={er_service_record_count}, incremental_id_res_circuit_breaker_min_records={incremental_id_res_circuit_breaker_min_records}, incremental_id_res_circuit_breaker_ratio={incremental_id_res_circuit_breaker_ratio}')

        # Run ER service if skip_ml=False rows exists
        if df_main_do_ml.filter(df_main_do_ml.skip_ml == False).count() > 0:     
            # Create the Athena/Glue table that is going to be used by ER service as data input
            create_or_update_er_input_table(job_args=job_args,
                incremental_common_schema_transformed_data_s3_path=incremental_common_schema_transformed_data_s3_path_do_ml,
                athena_tmp_results_path=athena_tmp_results_path,
                table_columns=df_main_do_ml.columns
            )

            schema_name = job_args["er_service_input_schema_name"]
            workflow_name = job_args['er_service_workflow_name']
            input_source_arn = job_args["er_input_source_arn"]
            output_s3_path = f"s3://{job_args['incremental_data_s3_bucket']}/er-resolved/{id_res_job_id}/"
            role_arn = job_args["ER_IAM_ROLE"]

            print('Starting ER management')
            create_or_update_schema_mapping_and_workflow(schema_name, workflow_name, input_source_arn, output_s3_path, role_arn, df_main_do_ml.columns)

            er_client = boto3.client('entityresolution')

            er_workflow = er_client.get_matching_workflow(
                workflowName=job_args['er_service_workflow_name']
            )

            incremental_er_results_path_override: str = job_args['incremental_er_results_path_override']
            if incremental_er_results_path_override.strip().lower() == 'not_overridden_use_default':
                er_job_id = start_matching_job(er_client=er_client, workflow_name=job_args['er_service_workflow_name'])

                print('er_job_id', er_job_id)
                print('ER management kicked off')

                # Path to s3 bucket containing ER results
                er_results_s3_path: str = os.path.join(er_workflow['outputSourceConfig'][0]['outputS3Path'],\
                    job_args['er_service_workflow_name'],
                    er_job_id,
                    'success'
                )
                print('er_results_s3_path', er_results_s3_path)

                # Define the expected Spark schema
                # ER service appends InputSourceARN and ConfidenceLevel to the ER service's output schema,
                # and appends RecordId and MatchId to the output schema
                er_results_schema =  StructType([
                    StructField("InputSourceARN", StringType(), True),
                    StructField("ConfidenceLevel", DoubleType(), True)])
                
                for field_name in [c['name'] for c in er_workflow['outputSourceConfig'][0]['output']]:
                    er_results_schema.add(StructField(field_name, StringType(), True))
                
                er_results_schema.add(StructField("RecordId", StringType(), True))
                er_results_schema.add(StructField("MatchID", StringType(), True))
                #print('Expected er_results_schema:')
                #print(er_results_schema)

                # Wait for the job to complete
                wait_for_job_completion(er_client, er_job_id, workflow_name)
            else:
                print(f'incremental_er_results_path_override is set to {incremental_er_results_path_override}.  Using these results instead of running ER service, but still loading ER results from newly defined ER workflow.')
                er_results_s3_path: str = incremental_er_results_path_override

                # Define the expected Spark schema
                # ER service appends InputSourceARN and ConfidenceLevel to the ER service's output schema,
                # and appends RecordId and MatchId to the output schema
                er_results_schema =  StructType([
                    StructField("InputSourceARN", StringType(), True),
                    StructField("ConfidenceLevel", DoubleType(), True)])
                
                for field_name in [c['name'] for c in er_workflow['outputSourceConfig'][0]['output']]:
                    er_results_schema.add(StructField(field_name, StringType(), True))
                
                er_results_schema.add(StructField("RecordId", StringType(), True))
                er_results_schema.add(StructField("MatchID", StringType(), True))

            # Read the ER results from S3
            df_er_results = spark_context.read.schema(er_results_schema).csv(er_results_s3_path, header=True, escape='"')

            df_er_results_pastuxid = df_er_results.join(
                df_ids_skip_ml,
                on='batch_record_id',
                how='left'
                )
            
            print('df_original_skipped_ml size_total: ', df_original_skipped_ml.count())
            print('df_er_results_pastuxid size_total: ', df_er_results_pastuxid.count())

            df_combined_results = df_er_results_pastuxid.unionByName(df_original_skipped_ml, allowMissingColumns=True) # ER results has more columns because ER service adds more columns
        else:
            print('Skipping ER service because no rows has skip_ml=false')
            df_combined_results = df_original_skipped_ml
            
            # Add some of the columns with null values that ER service would have added
            df_combined_results = df_combined_results \
                .withColumn('InputSourceARN', lit(None).cast(StringType())) \
                .withColumn('ConfidenceLevel', lit(None).cast(DoubleType())) \
                .withColumn('RecordId', lit(None).cast(StringType())) \
                .withColumn('MatchID', lit(None).cast(StringType()))          

        print('df_combined_results size_total: ', df_combined_results.count())
        #print('df_combined_results schema:')
        df_combined_results.printSchema()

        if job_args['debug_output_s3'].strip().lower() == 'true':
            comb_path = os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0110_id_df_incr_comb')
            df_combined_results.write.parquet(comb_path)
        
        # Apply Post-ML Rules        
        print('Running Post-ML Rules...')
        df_with_uid = IdResRuleManager("post_ml_rules", id_res_job_id, intermediate_s3_output_path, job_args['debug_output_s3']).applyRulesInOrder(df_combined_results)
        print('Running Post-ML Rules...Done')

        df_with_uid_persist = df_with_uid.persist(StorageLevel.DISK_ONLY) # prevent recalculation in almost all circumstances
        df_with_uid = df_with_uid_persist

        if job_args['debug_output_s3'].strip().lower() == 'true':
            post_ml_rule_data_s3_path: str = os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0120_df_post_ml_rules')
            df_with_uid.write.parquet(post_ml_rule_data_s3_path)

        # Rename fields df_batch, to mark them as "original" (vs df_with_uid with has data normalized by ER)
        original_cols = [col_name for col_name in df_batch.columns if col_name.startswith('ml_') or col_name.startswith('rules_')]

        print('df_batch.columns', df_batch.columns)
        print('original_cols', original_cols)
        df_batch_renamed = df_batch

        # rename the key columns in df_batch_renamed to something else
        df_batch_renamed = df_batch_renamed.withColumnRenamed('source_name', 'raw_source_name')
        df_batch_renamed = df_batch_renamed.withColumnRenamed('source_customer_id', 'raw_source_customer_id')

        if job_args['debug_output_s3'].strip().lower() == 'true':
            s1_path = os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0130_df_batch_renamed_operation_1')
            df_batch_renamed.write.option("header", True).parquet(s1_path)

        # Create a new DataFrame with renamed columns
        for col_name in df_batch.columns:
            if col_name.startswith('ml_') or col_name.startswith('rules_'):
                df_batch_renamed = df_batch_renamed.withColumnRenamed(col_name, f"original_{col_name}")

        if job_args['debug_output_s3'].strip().lower() == 'true':
            s2_path = os.path.join(intermediate_s3_output_path, 'debugging', id_res_job_id, '0130_df_batch_renamed_operation_2')
            df_batch_renamed.write.option("header", True).parquet(s2_path)

        # only keep the data attribute columns and the source_name and source_customer_id columns
        useful_cols = [col_name for col_name in df_batch_renamed.columns if col_name.startswith('original_') or col_name in ['raw_source_name', 'raw_source_customer_id','past_uxids']]
        df_batch_renamed = df_batch_renamed.select(*useful_cols)

        # Join the original data and ER results dataframes to get the original columns and normalized columns
        df_final = df_with_uid.join(df_batch_renamed, (df_with_uid.source_name == df_batch_renamed.raw_source_name) & (df_with_uid.source_customer_id == df_batch_renamed.raw_source_customer_id), 'inner').drop(df_batch_renamed.past_uxids)

        # convert uxid_confidence to double type
        df_final = df_final.withColumn("uxid_confidence", df_final["uxid_confidence"].cast(DoubleType()))

        # ensure updated_by_incremental_batch_id and updated_by_incremental_batch_description is set before output
        df_final = df_final.withColumn('updated_by_incremental_batch_id', lit(id_res_job_id))
        df_final = df_final.withColumn('updated_by_incremental_batch_description', lit('incremental_full_pipeline'))

        df_final_persist = df_final.persist(StorageLevel.DISK_ONLY) # prevent recalculation in almost all circumstances
        df_final = df_final_persist

        # snowflake_export output should have df_batch_output_bypass_id_res (scenarios 1 and 3 from exact search) included in the output dataframe
        df_snowflake_export = union_bypass_dataframe(df_final, df_batch_output_bypass_id_res).persist(StorageLevel.DISK_ONLY)

        print('df_snowflake_export schema')
        df_snowflake_export.printSchema()
        df_snowflake_export_count=df_snowflake_export.count()
        print('df_snowflake_export record count: ', df_snowflake_export_count)
        snowflake_export_path: str = f's3://{job_args["incremental_data_s3_bucket"]}/id-resolved/{id_res_job_id}/snowflake_export/'
        print(f'Writing snowflake_export output to "{snowflake_export_path}"...')
        df_snowflake_export.write.parquet(snowflake_export_path)
        print(f'Writing snowflake_export output to "{snowflake_export_path}"...Done')

        # OpenSearch results should NOT include df_batch_output_bypass_id_res (because these records are already written to OpenSearch right after exact search),
        # and records with null uxid should not be written to OpenSearch.
        df_opensearch_output = df_final.filter(col('uxid').isNotNull())

        print(f'Superbatch {id_res_job_id} write to OpenSearch...')

        # Write results back to OpenSearch
        df_opensearch_output.foreachPartition(lambda partition: write_spark_partition_to_opensearch(                 
            partition,
            host = job_args['ES_HOST'],
            iam_master_user_arn = job_args['ES_IAM_MASTER_USER_ARN'],
            region = job_args['ES_REGION'],
            index_name = opensearch_args['ES_GOLDEN_DB_INDEX_NAME'],
            record_fields = opensearch_args['RECORD_FIELDS'],
            metadata_fields = opensearch_args['METADATA_FIELDS'],
            batch_size = opensearch_args['ES_BATCH_SIZE']
            
        ))
        print(f'Superbatch {id_res_job_id} write to OpenSearch...Done')

        print(f'Superbatch {id_res_job_id} write entry to DDB table...')
        neptune_dydb_table_name = job_args['neptune_dydb_table']
        neptune_record_count = df_snowflake_export.filter(col('uxid').isNotNull()).count()
        ddb_response=dydb_neptune_put_item(neptune_dydb_table_name, 'ready-for-neptune-load', snowflake_export_path, id_res_job_id, neptune_record_count)
        print(f'Superbatch {id_res_job_id} write entry to DDB table...Done')
        print("ddb neptune entry done", ddb_response)
        
    except Exception as e:
        print('perform_id_resolution_er_and_after():', str(e))
        raise

def perform_id_resolution_single_batch(df_batch, spark_context):
    """
    Purpose:
        Perform id resolution on a single batch of candidate identities
    
    Inputs:
        - df_batch: spark dataframe of the batch of candidate identities

    Returns:
        df_batch_resolved: spark dataframe of the batch of candidate identities
    """
    id_res_job_id: str = datetime.now().strftime("%Y%m%d%H%M%S")
    print(f"value of id_res_job_id : {id_res_job_id}")

    batch_candidate_data_count: int = df_batch.count()
    
    df_batch_before_er, df_batch_output_bypass_id_res = perform_id_resolution_before_er(df_batch, spark_context, id_res_job_id)
    perform_id_resolution_er_and_after(df_batch_before_er, df_batch_output_bypass_id_res, spark_context, id_res_job_id, batch_candidate_data_count)
    