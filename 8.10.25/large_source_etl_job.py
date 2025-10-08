from awsglue.context import GlueContext
from pyspark.context import SparkContext
import sys
import boto3
import json
import time
import os, logging
import ast
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit, when, lpad, hour, minute

import time
from datetime import datetime
from pyspark.sql.functions import (
    col,
    lit,
    udf,
    monotonically_increasing_id,
)
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import current_timestamp
from pyspark.sql.column import Column

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Custom imports
from common_library import common_lib
from loewsClass import LoewsDataProcessor
from dynamoUtils import DynamoDBOperations
from triseptClass import TriseptDataProcessor
from uorGalaxyClass import UORGalaxyDataProcessor
from ushGalaxyClass import USHGalaxyDataProcessor
from denaliClass import DenaliDataProcessor
from uorWifiClass import uorWifiDataProcessor
from ushWifiClass import ushWifiDataProcessor
from colorvisionClass import colorvisionDataProcessor
from fnboClass import FNBODataProcessor
from triseptGuestbookClass import ushTriseptGuestbookDataProcessor
from commerceCartClass import uorCommerceCartDataProcessor
from adhocClass import AdhocDataProcessor

common_lib = common_lib

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "FileName",
        "FilePath",
        "Source",
        "s3_output_path",
        "dynamo_table_name",
        "glue_database_name",
        "glue_table_name",
    ],
)
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# args & Constants
output_cols = common_lib.common_schema
dynamo_table_name = args["dynamo_table_name"]
database_name = args["glue_database_name"]
glue_table_name = args["glue_table_name"]


def get_processor(source, glue_context):
    processors = {
        "UOR_LOEWS": LoewsDataProcessor,
        "UDX_TRISEPT": TriseptDataProcessor,
        "USH_DENALI": DenaliDataProcessor,
        "UOR_GALAXY": UORGalaxyDataProcessor,
        "USH_GALAXY": USHGalaxyDataProcessor,
        "UOR_WIFI": uorWifiDataProcessor,
        "USH_WIFI": ushWifiDataProcessor,
        "UOR_COLORVISION": colorvisionDataProcessor,
        "UOR_COMMERCE_CART": uorCommerceCartDataProcessor,
        "UDX_FNBO": FNBODataProcessor,
        "UDX_ADHOC": AdhocDataProcessor,
        "USH_TRISEPT": ushTriseptGuestbookDataProcessor
    }
    processor_class = processors.get(source)
    if processor_class:
        return processor_class(glue_context)
    raise ValueError(f"Unsupported source: {source}")



def clean_all_date_fields(df):
    """
    Standardizes all date and timestamp fields across sources.
    - Converts to ISO 8601 format (YYYY-MM-DD / YYYY-MM-DDTHH:MM:SSZ)
    - Nullifies all future values (year, month, day, time)
    """

    from datetime import datetime
    from pyspark.sql import functions as F

    now = datetime.now()

    # List of all known date/timestamp fields across all sources
    known_date_fields = [
        "birth_date", "BirthDate", "account_open_date", "AccountOpenDate",
        "opt_in_date", "OptInDate", "OptOutDate",
        "reservation_date", "last_modified_date", "guest_created_date",
        "guest_modified_date", "SourceSystemLastUpdateDate",
        "transaction_date", "checkout_timestamp",
        "record_created_timestamp", "record_last_updated_timestamp"
    ]

    # Select only the columns that exist in this dataframe
    date_cols = [c for c in known_date_fields if c in df.columns]

    for c in date_cols:
        # Parse multiple formats (both date and timestamp)
        df = df.withColumn(
            c,
            F.coalesce(
                F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col(c), "yyyy/MM/dd HH:mm:ss"),
                F.to_timestamp(F.col(c), "yyyyMMddHHmmss"),
                F.to_timestamp(F.col(c), "yyyy-MM-dd'T'HH:mm:ss"),
                F.to_date(F.col(c), "yyyy-MM-dd"),
                F.to_date(F.col(c), "yyyy/MM/dd"),
                F.to_date(F.col(c), "yyyyMMdd"),
                F.to_date(F.col(c), "ddMMyyyy"),
                F.to_date(F.col(c), "MM/dd/yyyy")
            )
        )

        # Nullify all future dates/times
        df = df.withColumn(
            c,
            F.when(
                (F.year(F.col(c)) > now.year) |
                ((F.year(F.col(c)) == now.year) & (F.month(F.col(c)) > now.month)) |
                ((F.year(F.col(c)) == now.year) & (F.month(F.col(c)) == now.month) &
                 (F.dayofmonth(F.col(c)) > now.day)) |
                ((F.year(F.col(c)) == now.year) & (F.month(F.col(c)) == now.month) &
                 (F.dayofmonth(F.col(c)) == now.day) &
                 ((F.hour(F.col(c)) > now.hour) |
                  ((F.hour(F.col(c)) == now.hour) & (F.minute(F.col(c)) > now.minute)) |
                  ((F.hour(F.col(c)) == now.hour) & (F.minute(F.col(c)) == now.minute) &
                   (F.second(F.col(c)) > now.second))))
            , F.lit(None)).otherwise(F.col(c))
        )

        # ISO 8601 formatting
        iso_format = "yyyy-MM-dd'T'HH:mm:ss'Z'" if "timestamp" in c.lower() or "time" in c.lower() else "yyyy-MM-dd"
        df = df.withColumn(c, F.date_format(F.col(c), iso_format))

    return df


def parse_file_paths(file_paths_str):
    """
    Parse the file paths from the input string.
    """
    file_paths_str = file_paths_str.strip().strip('"')
    try:
        return json.loads(file_paths_str)
    except json.JSONDecodeError:
        return [path.strip() for path in file_paths_str.split(",")]


try:
    #################Dynamo#################################
    FileName = args["FileName"]
    source = args["Source"]
    s3_output_path = args["s3_output_path"]
    # Construct the final output path
    s3_output_path = f"{s3_output_path.rstrip('/')}/{source}"
    # dynamo_in_progress(glue_context,FileName)
    FileName = ast.literal_eval(args["FileName"])
    dynamo_ops = DynamoDBOperations(glue_context, dynamo_table_name)
    dynamo_ops.dynamo_in_progress(FileName)
    ########################################################

    ##################### get file_paths #####################################

    # Parse the FilePath parameter
    file_paths_str = args["FilePath"]
    file_paths_str = file_paths_str.strip()
    if file_paths_str.startswith('"') and file_paths_str.endswith('"'):
        file_paths_str = file_paths_str[1:-1]

    try:
        # Try to parse as JSON
        file_paths = json.loads(file_paths_str)
    except json.JSONDecodeError:
        # If JSON parsing fails, split by comma
        file_paths = [path.strip() for path in file_paths_str.split(",")]

    # Ensure file_paths is a list of strings
    if isinstance(file_paths, list):
        file_paths = [path.strip('"') for path in file_paths]
    else:
        file_paths = [file_paths.strip('"')]
    # logger.info for debugging
    # logger.info("Processed file_paths:")
    for path in file_paths:
        logger.info(path)
    logger.info("file_paths are .... %s", file_paths)
    ########################################################
    # Main job to read files and process

    processor = get_processor(source, glue_context)
    # Use the processor to handle the data
    joined_tx_df = processor.run(file_paths, source)
    joined_tx_df = clean_all_date_fields(joined_tx_df)
    logger.info("joined_tx_df schema:")


    future_birthdays = joined_tx_df.filter(col("date_of_birth") > F.current_date())
    logger.info(f"Nullified {future_birthdays.count()} future date_of_birth entries.")

    joined_tx_df.printSchema()

    # Convert to respective timezone, files from same source can contain different source_name in each record. for example UOR_TRISEPT and USH_TRISEPT coming from UDX_Trisept
    # Log counts and sample timestamps by source before conversion
    logger.info("Counts and sample timestamps by source before conversion:")
    joined_tx_df.groupBy("source_name").count().show(truncate=False)
    joined_tx_df.select("source_name", "record_created_timestamp","record_last_updated_timestamp").show(
        5, truncate=False
    )
    # Convert both timestamps to respective timezone
    joined_tx_df = common_lib.convert_timestamp_to_utc(
        df=joined_tx_df,
        created_timestamp_column="record_created_timestamp",
        updated_timestamp_column="record_last_updated_timestamp",
        source=source,
    )
    
    # Log counts and sample timestamps by source after conversion
    logger.info("Counts and sample timestamps by source after conversion:")
    joined_tx_df.groupBy("source_name").count().show(truncate=False)
    joined_tx_df.select("source_name","record_created_timestamp", "record_last_updated_timestamp").show(
        5, truncate=False
    )
    # Create final dataframe with only output columns and NULL pass_thru_data
    df_final = joined_tx_df.select(
        *output_cols,
        lit(None).cast(StringType()).alias("pass_thru_data")
    )

    # # Create mono id to join with pass_thru columns
    # df_output = joined_tx_df.withColumn("row_id", monotonically_increasing_id())
    # pass_thru_cols = [c for c in joined_tx_df.columns if c not in output_cols]
    # logger.info("Pass-through columns:", pass_thru_cols)

    # create_map_udf = udf(
    #     lambda *cols: json.dumps(dict(zip(pass_thru_cols, cols))), StringType()
    # )

    # # Create df_pass_thru with the same unique identifier
    # df_pass_thru = joined_tx_df.select(
    #     monotonically_increasing_id().alias("row_id"),
    #     create_map_udf(*[joined_tx_df[c] for c in pass_thru_cols]).alias(
    #         "pass_thru_data"
    #     ),
    # )

    # df_final = df_output.join(df_pass_thru, on="row_id")
    # df_final = df_final.drop("row_id", *pass_thru_cols)

    # logger.info("df_final after removing pass-through columns:")
    # df_final.show(2, truncate=False)

    # Apply the column prefixes
    df_final = df_final.select(
        [col(c).alias(common_lib.get_col_prefix(c)) for c in df_final.columns]
    )

    glue_client = boto3.client("glue")

    # Capture and cache timestamp in df so its teh same in S3 and DynamoDB
    timestamp_df = spark.sql("SELECT current_timestamp() as ts")
    timestamp_value = timestamp_df.collect()[0]["ts"]
    logger.info("Captured timestamp value: %s", {timestamp_value})

    # Extract batch_id from file paths
    batch_ids = set()
    for path in file_paths:
        try:
            filename = path.split('/')[-1]
            potential_batch_id = filename.split('_')[0]
            if len(potential_batch_id) == 14 and potential_batch_id.isdigit():
                batch_ids.add(potential_batch_id)
        except Exception as e:
            logger.error("Error extracting batch_id from path %s: %s", {path}, {str(e)})

    if len(batch_ids) == 1:
        batch_id = batch_ids.pop()
        logger.info(f"Using batch_id: {batch_id}")
    else:
        logger.error(f"Invalid batch_ids found: {batch_ids}. Using timestamp as fallback.")
        batch_id = timestamp_value.strftime("%Y%m%d%H%M%S")

    
    # Add partition columns
    df_final = df_final.withColumn("tx_datetime", lit(timestamp_value))
    df_final = df_final.withColumn("year", lit(timestamp_value.year))
    df_final = df_final.withColumn("month", lit(timestamp_value.month))
    df_final = df_final.withColumn("day", lit(timestamp_value.day))
    df_final = df_final.withColumn("hour", hour("tx_datetime"))
    df_final = df_final.withColumn("minute", minute("tx_datetime"))
    df_final = df_final.withColumn("batch_id", lit(batch_id))  # Add batch_id column
    # Adding verification logging herew
    logger.info("Verifying hour and minute formatting:")
    df_final.select("hour", "minute").distinct().show()

    # Reorder columns, moving pass_thru_data to the end
    columns_order = [col for col in df_final.columns if col != "pass_thru_data"] + [
        "pass_thru_data"
    ]
    df_final = df_final.select(*columns_order)
    partition_columns = ["year", "month", "day", "hour", "minute", "batch_id"]  # Added batch_id
    # Create partition path using cached timestamp with proper formatting
    partition_values = {
    "year": str(timestamp_value.year),
    "month": str(timestamp_value.month),
    "day": str(timestamp_value.day),
    "hour": str(timestamp_value.hour),
    "minute": str(timestamp_value.minute),
    "batch_id": batch_id
        }

    logger.info("Checking partition column values:")
    df_final.select(*partition_columns).distinct().show()
    # logger.info("df_final is ")
    # df_final.show(2)
    df_final.cache()
    rec_count = df_final.count()

    # Write to S3 using DataFrame
    (df_final.write
    .mode("append")
    .partitionBy(partition_columns)  # Now includes batch_id
    .parquet(s3_output_path))

    

    # Build the partition path using the cached values including batch_id
    partition_path = "/".join(
        [f"{col}={partition_values[col]}" for col in partition_columns]
    )
    cmmn_sch_path = f"{s3_output_path}/{partition_path}/"

    logger.info(f"S3 write completed at: {datetime.now()}")
    logger.info(f"Written to common schema S3 path with partitions: {cmmn_sch_path}")
    logger.info(f"rec_count of transformed data: {rec_count}")

    # Adding delay for testing (configurable in seconds)
    delay_seconds = 1  # Adjust this value as needed
    logger.info(f"Adding {delay_seconds} seconds delay before DynamoDB update...")
    time.sleep(delay_seconds)
    logger.info(f"Starting DynamoDB update at: {datetime.now()}")
    logger.info(cmmn_sch_path)
    ############# for Dynamo Update###########################
    dynamo_ops = DynamoDBOperations(glue_context, dynamo_table_name)
    dynamo_ops.dynamo_complete(FileName, cmmn_sch_path, rec_count)


except Exception as e:
    dynamo_ops = DynamoDBOperations(glue_context, dynamo_table_name)
    dynamo_ops.dynamo_failed(FileName)
    raise e

job.commit()


