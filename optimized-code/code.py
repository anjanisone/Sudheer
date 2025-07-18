
import sys
import json
import logging
import boto3
import pytz
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from batch_data import get_next_candidate_identities_batch
from batch_data import get_next_candidate_identities_update_status_dydb, check_incr_control_job_status
from args_and_constants import get_job_args_incremental
from id_resolution import perform_id_resolution_single_batch
from id_res_utils import remove_all_cached_spark_dataframes
from pyspark.sql import functions as F
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Spark contexts and configs
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Performance tuning configs
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.default.parallelism", "400")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

job = Job(glueContext)

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s')
logger = logging.getLogger(__name__)

def should_shutdown_job(start_time):
    current_time = datetime.now(pytz.UTC)
    job_duration = current_time - start_time
    return job_duration > timedelta(hours=22)

def process_batch(df_batch, spark_context):
    try:
        perform_id_resolution_single_batch(df_batch, spark_context=spark_context)
        return "SUCCESS"
    except Exception as e:
        logger.error("Batch processing failed: %s", str(e))
        return "FAILED"

def main():
    try:
        job_start_time = datetime.now(pytz.UTC)
        job_args = get_job_args_incremental()

        dydb_table_name = job_args['dydb_table']
        max_items = job_args['max_items']
        max_records = job_args['max_records']
        sources = job_args['incremental_data_sources_enabled'].strip()
        incremental_override = job_args['incremental_common_schema_data_override']
        expire_in_days = int(job_args['expire_in_days'])
        ctrl_incr_key = job_args['ctrl_incr_key']
        incr_ctrl_table = job_args['incr_ctrl_table']

        tmp_spark_checkpoint_s3_path = f"s3://{job_args['temp_s3_bucket']}/tmp_spark_checkpoint/"
        logger.info('Setting temp spark checkpoint folder: %s', tmp_spark_checkpoint_s3_path)
        spark.sparkContext.setCheckpointDir(tmp_spark_checkpoint_s3_path)

        if incremental_override.lower().strip() != 'not_overridden_use_default':
            logger.info("Override path detected: %s", incremental_override)
            df_batch = spark.read.parquet(incremental_override)
            result = process_batch(df_batch, spark)
            logger.info(f"Batch Result (override): {result}")

        else:
            batch_results = []
            max_threads = 8  # Tune based on available DPUs and memory
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                while True:
                    if should_shutdown_job(job_start_time): 
                        logger.info("Shutting down the job due to 22 hour timeout.")
                        break

                    df_batch, batch_files = get_next_candidate_identities_batch(
                        dydb_table_name,
                        sources,
                        max_items,
                        max_records,
                        spark_context=spark,
                        glue_context=glueContext
                    )

                    if not df_batch:
                        logger.info("No more data batches to process. Exiting job.")
                        break

                    future = executor.submit(process_batch, df_batch, spark)
                    result = future.result()
                    logger.info(f"Batch Result: {result}")

                    ttl_value = int(time.time()) + (expire_in_days * 86400)
                    get_next_candidate_identities_update_status_dydb(
                        dydb_table_name, batch_files, expire_in_days,
                        "main-job-in-progress", "main-job-complete", ttl_value
                    )

                    remove_all_cached_spark_dataframes(spark, sc)

                    if check_incr_control_job_status(incr_ctrl_table, ctrl_incr_key).strip().lower() == "true":
                        logger.info("Control status set to TRUE. Exiting job.")
                        break

    except Exception as e:
        logger.error("Unhandled exception in main job: %s", str(e))
        if incremental_override.lower().strip() == 'not_overridden_use_default':
            get_next_candidate_identities_update_status_dydb(
                dydb_table_name, batch_files, expire_in_days,
                "main-job-in-progress", "main-job-failed"
            )
        raise

    finally:
        duration = datetime.now(pytz.UTC) - job_start_time
        logger.info("Total job duration: %s", str(duration))

if __name__ == "__main__":
    main()
