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
import logging,re
from common_library import common_lib, clean_all_date_fields
from pyspark.sql import functions as F
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
"""Didnt reuse the same processor for Galaxy because name_titles file is removed for USH, some columns are removed for Uor"""

class UORGalaxyDataProcessor:
    def __init__(self, glue_context):
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.common_lib = common_lib


        # Define file prefixes as named variables
        self.PREFIX_CONTACTS = "uo_galaxy_custcontacts"
        self.PREFIX_ADDRESS = "uo_galaxy_addresses"
        self.PREFIX_SUFFIX = "uo_galaxy_name_suffixes"
        self.PREFIX_TITLE = "uo_galaxy_name_titles"

        self.contacts_schema = StructType([
            StructField("custcontactid", StringType(), True),
            StructField("contacttype", StringType(), True),
            StructField("title", StringType(), True),
            StructField("salutation", StringType(), True),
            StructField("firstname", StringType(), True),
            StructField("middlename", StringType(), True),
            StructField("lastname", StringType(), True),
            StructField("addressid", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("unformattedphone", StringType(), True),
            StructField("fax", StringType(), True),
            StructField("cell", StringType(), True),
            StructField("email", StringType(), True),
            StructField("primarycontact", StringType(), True),
            StructField("noteid", StringType(), True),
            StructField("recordversion", StringType(), True),
            StructField("lastupdate", StringType(), True),
            StructField("externalid", StringType(), True),
            StructField("jobtitle", StringType(), True),
            StructField("nametitleid", StringType(), True),
            StructField("namesuffixid", StringType(), True),
            StructField("dob", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("unformattedlastname", StringType(), True),
            StructField("agegroup", StringType(), True),
            StructField("persistencestate", StringType(), True),
            StructField("identificationno", StringType(), True),
            StructField("allowemail", StringType(), True),
            StructField("custcontactguid", StringType(), True),
            StructField("lastupdatedby", StringType(), True),
            StructField("gxkeyid", StringType(), True),
            StructField("specialneeds", StringType(), True),
            StructField("deceased", StringType(), True)
        ])
        
        
        self.address_schema = StructType([
            StructField("addressid", StringType(), True),
            StructField("addressguid", StringType(), True),
            StructField("addresstype", StringType(), True),
            StructField("ownerid", StringType(), True),
            StructField("street1", StringType(), True),
            StructField("street2", StringType(), True),
            StructField("street3", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("postal", StringType(), True),
            StructField("countrycode", StringType(), True),
            StructField("addresscorrection", StringType(), True),
            StructField("persistencestate", StringType(), True),
            StructField("allowmailings", StringType(), True),
            StructField("recordversion", StringType(), True),
            StructField("lastupdate", StringType(), True),
            StructField("lastupdatedby", StringType(), True)
        ])
        
        
        
        self.title_schema = StructType([
            StructField("nametitleid", StringType(), True),
            StructField("nametitle", StringType(), True),
            StructField("recordversion", StringType(), True),
            StructField("lastupdate", StringType(), True),
            StructField("sequence", StringType(), True),
            StructField("inactive", StringType(), True),
            StructField("lastupdatedby", StringType(), True)
        ])
        
        
        self.suffix_schema = StructType([
            StructField("namesuffixid", StringType(), True),
            StructField("namesuffix", StringType(), True),
            StructField("recordversion", StringType(), True),
            StructField("lastupdate", StringType(), True),
            StructField("sequence", StringType(), True),
            StructField("inactive", StringType(), True),
            StructField("lastupdatedby", StringType(), True)
        ])
        
        
        
        
        # Define the list of file prefixes in the order of file_mapping_info doc
        self.file_prefixes = [
            self.PREFIX_CONTACTS,
            self.PREFIX_ADDRESS,
            self.PREFIX_TITLE,
            self.PREFIX_SUFFIX
                ]

        # Use the named prefix variables to define other configurations
        self.schemas = {
            self.PREFIX_CONTACTS: self.contacts_schema,
            self.PREFIX_ADDRESS: self.address_schema,
            self.PREFIX_TITLE: self.title_schema,
            self.PREFIX_SUFFIX: self.suffix_schema
                }

        self.output_cols = self.common_lib.common_schema

        self.columns_to_select = {
            self.PREFIX_ADDRESS: [
                "addressid",
                "street1",
                "street2", 
                "city",
                "state",
                "postal",
                "countrycode",
                "lastupdate"
            ],
            self.PREFIX_CONTACTS: [
                "custcontactid",
                "addressid",
                "firstname",
                "middlename",
                "lastname",
                "phone",
                "email",
                "dob",
                "gender",
                "lastupdate"
            ],
            self.PREFIX_TITLE: [
                "nametitleid",
                "nametitle",
                "lastupdate"
            ],
            self.PREFIX_SUFFIX: [
                "namesuffixid",
                "namesuffix",
                "lastupdate"
            ]
        }
        
        
        self.dedupekey = "lastupdate"

        
################# transformations #######################
        self.transformations = {
            self.PREFIX_ADDRESS: lambda df: df.select([F.upper(F.trim(F.col(c))).alias(c) for c in df.columns]),
            
            self.PREFIX_CONTACTS: lambda df: (
                df.withColumn(
                    "dob",
                    F.when(F.col("dob") == "1901-01-01", None)
                    .otherwise(
                        F.when(F.col("lastupdate") > "2019-07-31",
                                F.date_format(F.make_date(F.lit(1904), F.month(F.col("dob")), F.dayofmonth(F.col("dob"))), "yyyy-MM-dd"))
                        .otherwise(F.col("dob"))
                    )
                )
                .select(*[
                    (F.lower(F.trim(F.col(c))).alias(c) if c == "email" 
                    else F.upper(F.trim(F.col(c))).alias(c))
                    for c in df.columns
                ])
                .withColumn("email", 
                    F.when(F.length(F.col("email")) > 128, None)
                    .otherwise(F.col("email"))
                )
            ),
        
            self.PREFIX_TITLE: lambda df: df.select([F.upper(F.trim(F.col(c))).alias(c) for c in df.columns]),
        
            self.PREFIX_SUFFIX: lambda df: df.select([F.upper(F.trim(F.col(c))).alias(c) for c in df.columns])
        }
        
        
        
        
    @staticmethod
    def extract_date(filename):
        return regexp_extract(col("filename"), r"(\d{8})-FAKE\.csv$", 1)

    def extract_file_prefix(self, file_path: str) -> str:
        file_name = file_path.split("/")[-1].lower()
        for prefix in self.file_prefixes:
            if prefix.lower() in file_name:
                return prefix
        logger.info(f"Warning: No matching prefix found for file {file_path}")
        return None

    def process_and_join_files(self, filepaths: List[str]) -> DataFrame:

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
                    multiLine=True
                ).csv(file)
                # convert all column names to lower case to easily process files with Pascal case.
                for col_name in temp_df.columns:
                    temp_df = temp_df.withColumnRenamed(col_name, col_name.lower())
                print("tempdf columns are ..",temp_df.columns)
                

                # Check if the number of columns in the DataFrame matches the schema
                if len(temp_df.columns) != len(schema):
                    error_message = f"Number of columns in file ({len(temp_df.columns)}) does not match schema ({len(schema)}) for prefix: {prefix}"
                    logger.info(error_message)
                    print(schema)
                    temp_df.printSchema()
                    # temp_df.show(2)
                    # continue
                    raise ValueError(error_message)

                if df is None:
                    df = temp_df
                else:
                    df = df.union(temp_df)

            if df is not None:

                # Select the desired columns for this prefix
                if prefix in self.columns_to_select:
                    columns_to_keep = self.columns_to_select[prefix]
                    # Check if all specified columns exist in the DataFrame
                    missing_columns = [
                        col for col in columns_to_keep if col not in df.columns
                    ]
                    if missing_columns:
                        logger.error(f"ERROR: The following columns are not present in the DataFrame for prefix '{prefix}': {missing_columns}")
                        raise ValueError("Missing columns in the DataFrame")

                    if columns_to_keep:
                        df = df.select(*columns_to_keep)
                        logger.info(f"Selected columns for prefix '{prefix}': {columns_to_keep}")
                    else:
                        logger.error(f"ERROR: No valid columns to select for file prefix '{prefix}'.")
                        raise ValueError("No valid columns to select")
                else:
                    logger.error(f"ERROR: No columns specified for file prefix '{prefix}'.")
                    raise ValueError("No columns specified")


                # Apply transformations
                if prefix in self.transformations:
                    df = self.transformations[prefix](df)
                    logger.info(f"Applied transformations for prefix '{prefix}'")
                else:
                    logger.warning(f"Warning: No transformations specified for prefix '{prefix}'")

                df = clean_all_date_fields(df, ['dob'])

                # Handle duplicates, keeping the row with the maximum value in the dedupekey column
                if self.dedupekey in df.columns:
                    df_deduped = self.common_lib.deduplicate_by_timestamp(
                        df=df,
                        dedupekey=self.dedupekey,
                        timestamp_format="yyyy-MM-dd HH:mm:ss.SSS",
                        validate_timestamps=True,
                        drop_invalid_timestamps=False,
                        preserve_case=True
                    )
                    df = df_deduped
                    
                    # Drop dedupekey column for all prefixes except "uo_galaxy_custcontacts"
                    if prefix != "uo_galaxy_custcontacts":
                        # Get case-insensitive match for the dedupekey column
                        columns_lower = [col_name.lower() for col_name in df.columns]
                        if self.dedupekey.lower() in columns_lower:
                            # Find the actual column name with original case
                            actual_column = df.columns[columns_lower.index(self.dedupekey.lower())]
                            logger.info(f"Dropping column '{actual_column}' (matched case-insensitively with '{self.dedupekey}')")
                            df = df.drop(actual_column)
                        else:
                            logger.warning(
                                f"Warning: Could not find column '{self.dedupekey}' (case-insensitive) "
                                f"for dropping. Available columns: {', '.join(df.columns)}"
                            )
                else:
                    logger.warning(
                        f"Warning: {self.dedupekey} column (case-insensitive) not found in DataFrame "
                        f"for prefix '{prefix}'. Available columns: {', '.join(df.columns)}. "
                        "Skipping deduplication."
                    )
                
                logger.info(f"Processed file group with prefix: {prefix}")
                # Add the processed DataFrame to the dictionary
                df_dict[prefix] = df
                print("df_dict is ....", df_dict)
                
                

        # Define join mapping
        join_mapping = {
            "uo_galaxy_custcontacts": "addressid",
            "uo_galaxy_addresses": "addressid",
            "uo_galaxy_name_suffixes": "namesuffixid",
            "uo_galaxy_name_titles": "nametitleid"
        }
        

        # Perform the join
        final_df = self.join_dataframes(df_dict, join_mapping)

        # Debug: logger.info schema of final_df
        logger.info("Schema of final_df after join:")
        final_df.printSchema()

        return final_df

    def join_dataframes(self, df_dict, join_mapping):
        # Start with the uo_galaxy_custcontacts DataFrame as the base
        base_df_name = 'uo_galaxy_custcontacts'
        base_df = df_dict[base_df_name]
        base_df = base_df.alias('base')  # Add alias to base DataFrame
        
        base_join_key = join_mapping[base_df_name]
        
        # Join with other DataFrames
        for df_name, join_key in join_mapping.items():
            if df_name != base_df_name:  # Skip the base DataFrame
                df = df_dict[df_name].alias(df_name)  # Add alias to the DataFrame being joined
                base_df = base_df.join(
                    df,
                    base_df[f'base.{base_join_key}'] == df[f'{df_name}.{join_key}'],
                    'left'
                ).drop(df[join_key])  # Drop the join_key column from the joined DataFrame
        
        # Select columns, keeping addressid and lastupdate from the base DataFrame
        columns_to_select = []
        for column in base_df.columns:
            if column.endswith('.addressid') or column.endswith('.lastupdate'):
                if column.startswith('base.'):
                    columns_to_select.append(col(column).alias(column.split('.')[-1]))
            else:
                columns_to_select.append(col(column))
            
        final_df = base_df.select(*columns_to_select)
        
        return final_df

    def map_to_common_schema(self, source_df: DataFrame,source) -> DataFrame:
        # Define the mapping from source columns to target columns

        column_mapping = {
            "source_name": lit(source), 
            "source_customer_id": col("custcontactid"),
            "prefix": lit(""),
            "first_name": col("firstname"),
            "middle_name": col("middlename"),
            "last_name": col("lastname"),
            "generation_qualifier": lit(""), 
            "gender": col("gender"),  
            "birth_date": col("dob"),
            "address_line_1": col("street1"),
            "address_line_2": col("street2"),
            "city": col("city"),
            "state_code": col("state"),
            "postal_code": col("postal"),
            "country_code": col("countrycode"),
            "phone": col("phone"), 
            "email_address": col("email"),  # Using the EMAIL column
            "guest_id": lit(""), 
            "record_created_timestamp": lit(""), # Not present in source
            "record_last_updated_timestamp": col("lastupdate"),
        }

        # Create a list of columns for the target DataFrame
        target_columns = []

        # Add mapped columns
        for col_name in self.output_cols:
            target_columns.append(
                column_mapping.get(col_name, lit("")).alias(col_name)
            )

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
