import sys
import json, ast
from typing import List
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import regexp_extract, col, input_file_name
from pyspark.sql.functions import upper, lower, trim, length, when, col, max, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, col, coalesce
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import input_file_name, regexp_extract, col
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, expr
from pyspark.sql.column import Column
import logging, re
from pyspark.sql.functions import md5, concat_ws, col
from pyspark.sql.functions import (
    col, 
    length, 
    when, 
    max, 
    count, 
    date_format, 
    to_timestamp, 
    collect_list,
    lit,
    col,
    expr
)


from common_library import common_lib, clean_all_date_fields
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AdhocDataProcessor:
    def __init__(self, glue_context):
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.common_lib = common_lib

        # Define file prefixes as named variables
        self.PREFIX_udx_adhoc = "udx_adhoc"
        self.udx_adhoc_schema = StructType([
            StructField("TitleName", StringType(), True),
            StructField("FirstName", StringType(), True),
            StructField("LastName", StringType(), True),
            StructField("SuffixName", StringType(), True),
            StructField("AddressLineName1", StringType(), True),
            StructField("AddressLineName2", StringType(), True),
            StructField("AddressLineName3", StringType(), True),
            StructField("CityName", StringType(), True),
            StructField("StateCode", StringType(), True),
            StructField("PostalCode", StringType(), True),
            StructField("CountryCode", StringType(), True),
            StructField("DMACode", StringType(), True),
            StructField("PointOfOriginCode", StringType(), True),
            StructField("HomePhoneNumber", StringType(), True),
            StructField("MobilePhoneNumber", StringType(), True),
            StructField("EmailAddress", StringType(), True),
            StructField("BirthDate", StringType(), True),
            StructField("GenderCode", StringType(), True),
            StructField("HalloweenHorrorNightMobileOptInFlag", StringType(), True),
            StructField("HalloweenHorrorNightOptInflag", StringType(), True),
            StructField("RockTheUniverseMobileOptInflag", StringType(), True),
            StructField("RockTheUniverseOptInFlag", StringType(), True),
            StructField("UniversalOrlandoMobileOptInFlag", StringType(), True),
            StructField("UniversalOrlandoOptInFlag", StringType(), True),
            StructField("WizardingWorldofHarryPorterOptInFlag", StringType(), True),
            StructField("CurrentAnnualPassNumber", StringType(), True),
            StructField("AnnualPassTypeName", StringType(), True),
            StructField("AnnualPassValidDays", StringType(), True),
            StructField("AnnualPassValidDate", StringType(), True),
            StructField("AnnualPassPaymentTypeCode", StringType(), True),
            StructField("SourceSystemLastUpdateDate", StringType(), True),
            StructField("InteractionTypeName", StringType(), True),
            StructField("InteractionDate", StringType(), True),
            StructField("InteractionTime", StringType(), True),
            StructField("InteractionSourceSystemName", StringType(), True),
            StructField("InteractionSourceSystemColumnName", StringType(), True),
            StructField("InteractionSourceSystemNameColumnValue", StringType(), True),
            StructField("InteractionOptInSystemName", StringType(), True),
            StructField("InteractionOptInSourceName", StringType(), True)
        ])
        

        # Define the list of file prefixes in the order of file_mapping_info doc
        self.file_prefixes = [self.PREFIX_udx_adhoc]

        # Use the named prefix variables to define other configurations
        self.schemas = {self.PREFIX_udx_adhoc: self.udx_adhoc_schema}

        self.output_cols = self.common_lib.common_schema
        self.dedupekey = "SourceSystemLastUpdateDate"

        self.columns_to_select = {
            self.PREFIX_udx_adhoc: [
                "TitleName",
                "FirstName",
                "LastName",
                "SuffixName",
                "AddressLineName1",
                "AddressLineName2",
                "CityName",
                "StateCode",
                "PostalCode",
                "CountryCode",
                "HomePhoneNumber",
                "MobilePhoneNumber",
                "EmailAddress",
                "BirthDate",
                "GenderCode",
                "SourceSystemLastUpdateDate"
            ]
        }
        

        self.transformations = {
            self.PREFIX_udx_adhoc: lambda df: df.withColumn(
                "EmailAddress",
                when(length(col("EmailAddress")) > 128, None)
                .otherwise(F.lower(F.trim(col("EmailAddress"))))
            ).select([F.upper(F.trim(F.col(c))).alias(c) if c != "EmailAddress" else F.col("EmailAddress").alias(c) 
                    for c in df.columns])
        }
        

    # @staticmethod
    # def extract_date(filename):
    #     return regexp_extract(col("filename"), r"(\d{8})-FAKE\.csv$", 1)

    def extract_file_prefix(self, file_path: str) -> str:
        file_name = file_path.split("/")[-1].lower()
        for prefix in self.file_prefixes:
            if prefix.lower() in file_name:
                return prefix
        logger.info(f"Warning: No matching prefix found for file {file_path}")
        return None

    def standardize_adhoc_timestamp(self, df: DataFrame) -> DataFrame:
        """
        Standardize Adhoc timestamp format by:
        1. Identifying duplicate records (excluding SourceSystemLastUpdateDate)
        2. For duplicates, keeping records with seconds(i.e) ISO format when available
        3. Standardizing remaining records without seconds to inclu' seconds
        """
        try:
            if self.dedupekey not in df.columns:
                return df
    
            logger.info("Starting Adhoc timestamp standardization process...")
            
            # Log initial state
            initial_count = df.count()
            logger.info(f"Initial record count: {initial_count}")
    
            # 1. Get all columns except SourceSystemLastUpdateDate for duplicate checking
            dedupe_columns = [c for c in df.columns if c != self.dedupekey]
    
            # 2. Identify duplicate groups and mark records with/without seconds
            window_spec = Window.partitionBy(dedupe_columns)
            
            df_marked = df.withColumn(
                "has_seconds",
                length(col(self.dedupekey)) > 19 ## This record has seconds 
            ).withColumn(
                "group_has_records_with_seconds",
                max("has_seconds").over(window_spec) ## if this group of records is duplicate ignoring the dedupe key
            ).withColumn(
                "record_count_in_group",
                count("*").over(window_spec) ## how many records are there within the group
            )
    
            # 3. Apply the filtering logic:
            # - Keep records with seconds if they exist in the group
            # - Keep all records if group has no records with seconds
            df_filtered = df_marked.filter(
                (col("record_count_in_group") == 1) |  
                (
                    (col("record_count_in_group") > 1) &  
                    (
                        (~col("group_has_records_with_seconds")) | 
                        (col("has_seconds"))  
                    )
                )
            )

            # 4. Standardize remaining records without seconds
            df_standardized = df_filtered.withColumn(
                self.dedupekey,
                when(
                    ~col("has_seconds"),
                    date_format(
                        coalesce(to_timestamp(col(self.dedupekey), "M/d/yyyy"),to_timestamp(col(self.dedupekey),"yyyy-MM-dd")),
                        "yyyy-MM-dd HH:mm:ss.SSS"
                    )
                ).otherwise(col(self.dedupekey))
            ).drop("has_seconds", "group_has_records_with_seconds", "record_count_in_group")
    
            # Log statistics
            final_count = df_standardized.count()
            records_removed = initial_count - final_count
            
            logger.info("Timestamp standardization statistics:")
            logger.info(f"- Initial record count: {initial_count}")
            logger.info(f"- Final record count: {final_count}")
            logger.info(f"- Duplicate records removed: {records_removed}")
            
            if records_removed > 0:
                removal_percentage = (records_removed / initial_count) * 100
                logger.info(f"- Percentage of records removed: {removal_percentage:.2f}%")
    
            return df_standardized
    
        except Exception as e:
            logger.error(f"Error standardizing Adhoc timestamp: {str(e)}")
            raise
    

    def process_and_join_files(self, filepaths: List[str]) -> DataFrame:
        # Check if all files are CSV, there are some .xlsx files being delivered in S3
        non_csv_files = [file for file in filepaths if not file.lower().endswith('.csv')]
        if non_csv_files:
            logger.warning(f"Skipping process - Found non-CSV files: {non_csv_files}")
            return None

        # Group files by their base name (without part suffix)
        file_groups = {}
        for file in filepaths:
            base_name = re.sub(r"_\d{3}_of_\d{3}\.csv$", ".csv", file)
            if base_name not in file_groups:
                file_groups[base_name] = []
            file_groups[base_name].append(file)
        logger.info("File groups:", file_groups)

        # Process files and create DataFrame dictionary
        df_dict = {}
        for base_name, files in file_groups.items():
            prefix = self.extract_file_prefix(base_name)
            if prefix is None:
                error_message = f"These files have no matching prefix: {base_name}"
                raise ValueError(error_message)

            schema = self.schemas[prefix]

            # Read and union all part files
            df = None
            for file in files:
                temp_df = self.spark.read.options(
                    delimiter=",",
                    header=True,
                    quote='"',
                    escape='"',
                    multiLine=True,
                    inferSchema=False
                ).schema(self.udx_adhoc_schema).csv(file)
                # Rename columns to match the schema
                column_names = [field.name for field in self.udx_adhoc_schema.fields]
                temp_df = temp_df.toDF(*column_names)
            

                # Check if the number of columns in the DataFrame matches the schema
                if len(temp_df.columns) != len(schema):
                    error_message = f"Number of columns in file ({len(temp_df.columns)}) does not match schema ({len(schema)}) for prefix: {prefix}"
                    raise ValueError(error_message)

                if df is None:
                    df = temp_df
                else:
                    df = df.union(temp_df)

            # Process the DataFrame
            if df is not None:
                # Select the desired columns
                if prefix in self.columns_to_select:
                    columns_to_keep = self.columns_to_select[prefix]
                    df = df.select(*columns_to_keep)
                    logger.info(f"Selected columns for prefix '{prefix}': {columns_to_keep}")
                    # Add source_name column based on filename after selecting columns
                    df = df.withColumn(
                        "source_name",
                        when(lower(lit(file)).contains("uor"), lit("UOR_ADHOC"))
                        .when(lower(lit(file)).contains("ush"), lit("USH_ADHOC"))
                        .when(lower(lit(file)).contains("uhu"), lit("UHULV_ADHOC"))
                        .when(lower(lit(file)).contains("ukr"), lit("UKRFR_ADHOC"))
                        .when(lower(lit(file)).contains("uhulv"), lit("UHULV_ADHOC"))
                        .when(lower(lit(file)).contains("ukrfr"), lit("UKRFR_ADHOC"))
                        .otherwise(lit("UDX_ADHOC"))
                    )
                else:
                    raise ValueError(f"No columns specified for file prefix '{prefix}'.")

                # Apply transformations
                if prefix in self.transformations:
                    df = self.transformations[prefix](df)
                    logger.info(f"Applied transformations for prefix '{prefix}'")
                else:
                    logger.warning(f"No transformations specified for prefix '{prefix}'")

                df = clean_all_date_fields(df, ["BirthDate"])

                # Handle duplicates, keeping the row with the maximum value in the dedupekey column
                if self.dedupekey in df.columns:
                    df = self.standardize_adhoc_timestamp(df)

                    df_deduped = self.common_lib.deduplicate_by_timestamp(
                    df=df,
                    dedupekey=self.dedupekey,
                    # timestamp_format= "M/d/yyyy",
                    timestamp_format= "yyyy-MM-dd HH:mm:ss.SSS",
                    validate_timestamps=True,
                    drop_invalid_timestamps=False,
                    preserve_case=True
                    )
                    df=df_deduped

                # Create list of columns excluding only 'SourceSystemLastUpdateDate' 
                # INFO: 1/22/25: removing "homephonenumber", "mobilephonenumber"
                columns_to_hash = [
                    c
                    for c in df.columns
                    if c.lower() not in ["sourcesystemlastupdatedate", "homephonenumber", "mobilephonenumber","source_name"]
                ]
                
                logger.info("Columns to hash:", columns_to_hash)
                
                # Add the derived column 'source_customer_id' with MD5 hash of filtered columns, coalesce to convert nulls to empty strings (ex: John||Newyork)
                df = df.withColumn(
                    "source_customer_id",
                    md5(concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in columns_to_hash]))
                )
                
                logger.info(f"Processed file group with prefix: {prefix}")
                logger.info("Schema of final DataFrame:")
                df.printSchema()
                
                return df
            else:
                raise ValueError("No data processed")


    def map_to_common_schema(self, source_df: DataFrame,source) -> DataFrame:
        # Define the mapping from source columns to target columns

        column_mapping = {
            "source_name": col("source_name"),  
            "source_customer_id": col("source_customer_id"),
            "prefix": col("TitleName"),
            "first_name": col("FirstName"),
            "middle_name": lit(""), 
            "last_name": col("LastName"),
            "generation_qualifier": lit(""), 
            "gender": col("GenderCode"),
            "birth_date": col("BirthDate"),
            "address_line_1": col("AddressLineName1"),
            "address_line_2": col("AddressLineName2"),
            "city": col("CityName"),
            "state_code": col("StateCode"),
            "postal_code": col("PostalCode"),
            "country_code": col("CountryCode"),
            "phone": col("HomePhoneNumber"),  # Using HomePhoneNumber as primary phone
            "email_address": col("EmailAddress"),
            "guest_id": lit(""), 
            "record_created_timestamp": lit(""),  # Not in source schema
            "record_last_updated_timestamp": col("SourceSystemLastUpdateDate"),
        }
        

        # Create a list of columns for the target DataFrame
        target_columns = []

        # Add mapped columns
        for col_name in self.output_cols:
            target_columns.append(column_mapping.get(col_name, lit("")).alias(col_name))

        # Get the source column names from the column_mapping
        mapped_source_columns = set()
        for value in column_mapping.values():
            if isinstance(value, Column):
                mapped_source_columns.add(value._jc.toString())

        # Add extra columns from source_df that are not in the mapping
        extra_columns = [
            col(c) for c in source_df.columns if c not in mapped_source_columns
        ]
        target_columns.extend(extra_columns)

        # Create the target DataFrame
        target_df = source_df.select(target_columns)
        return target_df

    def run(self, filepaths: List[str],source) -> DataFrame:
        """
        Main method to run the entire data processing pipeline.
        """
        result_df = self.process_and_join_files(filepaths)
        output_df = self.map_to_common_schema(result_df,source)
        return output_df
    