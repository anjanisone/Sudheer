from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, col, expr
import logging, re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    trim,
    upper,
    lower,
    length,
    col,
    when,
    lit,
    concat_ws,
    substring,
    locate,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from typing import List
from pyspark.sql.column import Column
from common_library import common_lib, clean_all_date_fields
from pyspark.sql.functions import when, col, substring, locate, length, expr, when
from pyspark.sql.functions import (
    max as spark_max,
    min as spark_min,
)  # max , min are interfering with python native max in self.expected_lengths


# moved these 2 functions outside of Class because spart context is getting broadcasted.


def parse_line_generic(line, expected_length, positions):
    if len(line) != expected_length:
        raise ValueError(
            f"Invalid line length. Expected {expected_length}, got {len(line)}"
        )
    return tuple(line[start:end].strip() for start, end in positions)


def create_parse_function(expected_length, positions):
    def parse_function(line):
        return parse_line_generic(line, expected_length, positions)

    return parse_function


""" This parsing function, when called with a line of text from the file, will know how to parse that line according to the expected structure 
    of an "input" file.

    The importance of this approach is that it allows for different parsing logic for each file type, while using a common processing method. 
    The process_file method doesn't need to know the specifics of how each file type is parsed; it just needs to know how to retrieve and use the correct parsing function for each file type. """


class FNBODataProcessor:
    def __init__(self, glue_context):
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        # Define file prefixes as named variables
        self.PREFIX_cif = "cif"
        self.PREFIX_naf = "naf"
        self.PREFIX_amf = "amf"

        # self.file_prefixes = ["cif", "naf", "amf"]
        self.file_prefixes = [self.PREFIX_cif, self.PREFIX_naf, self.PREFIX_amf]

        self.common_lib = common_lib
        self.cif_schema = StructType(
            [
                StructField("record_type", StringType(), True),
                StructField("arn", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("middle_initial", StringType(), True),
                StructField("prefix", StringType(), True),
                StructField("suffix", StringType(), True),
                StructField("address_line_1", StringType(), True),
                StructField("address_line_2", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("zip_code", StringType(), True),
                StructField("filler1", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("account_open_date", StringType(), True),
                StructField("card_type", StringType(), True),
                StructField("first_activation_date", StringType(), True),
                StructField("email", StringType(), True),
                StructField("filler4", StringType(), True),
                StructField("filler5", StringType(), True),
                StructField("filler6", StringType(), True),
                StructField("partner_reward_id", StringType(), True),
                StructField("rewards_program", StringType(), True),
                StructField("filler7", StringType(), True),
                StructField("filler8", StringType(), True),
                StructField("filler9", StringType(), True),
                StructField("filler10", StringType(), True),
            ]
        )

        self.naf_schema = StructType(
            [
                StructField("record_type", StringType(), True),
                StructField("arn", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("middle_initial", StringType(), True),
                StructField("prefix", StringType(), True),
                StructField("suffix", StringType(), True),
                StructField("filler1", StringType(), True),
                StructField("address_line_1", StringType(), True),
                StructField("address_line_2", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("zip_code", StringType(), True),
                StructField("primary_phone_number", StringType(), True),
                StructField("reward_member_id", StringType(), True),
                StructField("filler2", StringType(), True),
                StructField("email", StringType(), True),
                StructField("filler3", StringType(), True),
                StructField("acquisition_promo_code", StringType(), True),
                StructField("agent_sub_agent", StringType(), True),
                StructField("filler4", StringType(), True),
                StructField("filler5", StringType(), True),
                StructField("filler6", StringType(), True),
                StructField("filler7", StringType(), True),
                StructField("filler8", StringType(), True),
            ]
        )

        self.amf_schema = StructType(
            [
                StructField("record_type", StringType(), True),
                StructField("arn", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("middle_initial", StringType(), True),
                StructField("prefix", StringType(), True),
                StructField("suffix", StringType(), True),
                StructField("filler1", StringType(), True),
                StructField("address_line_1", StringType(), True),
                StructField("address_line_2", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("zip_code", StringType(), True),
                StructField("primary_phone_number", StringType(), True),
                StructField("reward_member_id", StringType(), True),
                StructField("reward_member_id_response_code", StringType(), True),
                StructField("email", StringType(), True),
                StructField("filler2", StringType(), True),
                StructField("acquisition_promo_code", StringType(), True),
                StructField("agent_sub_agent", StringType(), True),
                StructField("update_type", StringType(), True),
                StructField("previous_record_identifier", StringType(), True),
                StructField("filler3", StringType(), True),
            ]
        )

        # Use the named prefix variables to define other configurations
        self.schemas = {
            self.PREFIX_cif: self.cif_schema,
            self.PREFIX_naf: self.naf_schema,
            self.PREFIX_amf: self.amf_schema,
        }
        self.cif_fixed_width_positions = [
            (0, 1),  # Record Type (1)
            (1, 17),  # ARN (16)
            (17, 37),  # Last Name (20)
            (37, 52),  # First Name (15)
            (52, 53),  # Middle Initial (1)
            (53, 63),  # Prefix (10)
            (63, 73),  # Suffix (10)
            (73, 113),  # Address Line 1 (40)
            (113, 153),  # Address Line 2 (40)
            (153, 173),  # City (20)
            (173, 176),  # State (3)
            (176, 189),  # Zip Code (13)
            (189, 199),  # Filler (10)
            (199, 209),  # Primary Phone Number (10)
            (209, 217),  # Account Open Date (8)
            (217, 220),  # Card Type (3)
            (220, 228),  # First Activation Date (8)
            (228, 348),  # Email (120)
            (348, 351),  # Filler (3)
            (351, 354),  # Filler (3)
            (354, 356),  # Filler (2)
            (356, 381),  # Partner Reward ID (25)
            (381, 384),  # Rewards Program (3)
            (384, 393),  # Filler (9)
            (393, 401),  # Filler (8)
            (401, 426),  # Filler (25)
            (426, 550),  # Filler (124)
        ]

        self.naf_fixed_width_positions = [
            (0, 1),  # Record Type (1)
            (1, 17),  # ARN (16)
            (17, 37),  # Last Name (20)
            (37, 52),  # First Name (15)
            (52, 53),  # Middle Initial (1)
            (53, 63),  # Prefix (10)
            (63, 73),  # Suffix (10)
            (73, 98),  # Filler (25)
            (98, 123),  # Address Line 1 (25)
            (123, 148),  # Address Line 2 (25)
            (148, 173),  # City (25)
            (173, 176),  # State (3)
            (176, 185),  # Zip Code (9)
            (185, 195),  # Primary Phone Number (10)
            (195, 220),  # Reward Member ID (25)
            (220, 221),  # Filler (1)
            (221, 341),  # Email (120)
            (341, 343),  # Filler (2)
            (343, 349),  # Acquisition Promo Code (6)
            (349, 355),  # Agent/Sub Agent (6) - Corrected from 7 to 6
            (355, 364),  # Filler (9)
            (364, 372),  # Filler (8)
            (372, 380),  # Filler (8)
            (380, 405),  # Filler (25)
            (405, 500),  # Filler (95)
        ]

        self.amf_fixed_width_positions = [
            (0, 1),  # Record Type (1)
            (1, 17),  # ARN (16)
            (17, 37),  # Last Name (20)
            (37, 52),  # First Name (15)
            (52, 53),  # Middle Initial (1)
            (53, 63),  # Prefix (10)
            (63, 73),  # Suffix (10)
            (73, 98),  # Filler (25)
            (98, 123),  # Address Line 1 (25)
            (123, 148),  # Address Line 2 (25)
            (148, 173),  # City (25)
            (173, 176),  # State (3)
            (176, 185),  # Zip Code (9)
            (185, 195),  # Primary Phone Number (10)
            (195, 220),  # Reward Member ID (25)
            (220, 221),  # Reward Member ID Response Code (1)
            (221, 341),  # Email (120)
            (341, 343),  # Filler (2)
            (343, 349),  # Acquisition Promo Code (6)
            (349, 355),  # Agent/Sub Agent (6)
            (355, 356),  # Update Type (1)
            (356, 372),  # Previous Record Identifier (16)
            (372, 500),  # Filler (128) - Combined last filler fields
        ]

        # Calculate expected lengths
        self.expected_lengths = {
            "naf": max(end for _, end in self.naf_fixed_width_positions),
            "amf": max(end for _, end in self.amf_fixed_width_positions),
            "cif": max(end for _, end in self.cif_fixed_width_positions),
        }
        self.transformations = {
            "cif": lambda df: self.common_lib.validate_email(
                self.common_lib.trim_upper(df), "email", 128
            ),
            "amf": lambda df: df,
            "naf": lambda df: (
                df.withColumn(
                    "resort_area_code",
                    F.when(F.col("agent_sub_agent").isNull(), None)
                    .when(
                        F.substring(F.trim(F.col("agent_sub_agent")), 1, 3) == "UFL",
                        "UOR",
                    )
                    .when(
                        F.substring(F.trim(F.col("agent_sub_agent")), 1, 3) == "USH",
                        "USH",
                    )
                    .otherwise(None),
                )
            ),
        }  # left 3 characters of agent_sub_agent to check resort area code

        self.columns_to_select = {
            "cif": [
                "arn",
                "last_name",
                "first_name",
                "middle_initial",
                "prefix",
                "suffix",
                "address_line_1",
                "address_line_2",
                "city",
                "state",
                "zip_code",
                "email",
                "account_open_date",
                "phone",
            ],
            "naf": [
                "arn",
                "agent_sub_agent",
            ],  # "resort_area_code" is a derived field from agent_sub_agent so transforming it below
            "amf": [
                "arn",
                "previous_record_identifier",
            ],  
        }

        self.join_keys = ["arn", "arn"]
        self.base_df_name = "cif"
        # Create parsing functions
        self.parse_functions = {
            "naf": create_parse_function(
                self.expected_lengths["naf"], self.naf_fixed_width_positions
            ),
            "amf": create_parse_function(
                self.expected_lengths["amf"], self.amf_fixed_width_positions
            ),
            "cif": create_parse_function(
                self.expected_lengths["cif"], self.cif_fixed_width_positions
            ),
        }
        self.output_cols = self.common_lib.common_schema

    def get_transformations(self, df, file_type):
        if file_type in self.transformations:
            df = self.transformations[file_type](df)

    def extract_file_prefix(self, file_path: str) -> str:
        file_name = file_path.split("/")[-1].lower()
        for prefix in self.file_prefixes:
            if prefix.lower() in file_name:
                return prefix
        logger.error(f"Warning: No matching prefix found for file {file_path}")
        return None

    def process_file(self, file_path):
        # Determine the file type based on the file prefix
        file_prefix = self.extract_file_prefix(file_path)
        logger.info("file_prefix is ....", file_prefix)

        if file_prefix not in self.file_prefixes:
            logger.info(f"Warning: Unrecognized file prefix for {file_path}")
            return None

        # Get the corresponding schema and parse function
        schema = getattr(self, f"{file_prefix}_schema")
        # logger.info(f"schema for {file_prefix} is ...\n {schema}")
        parse_function = self.parse_functions[file_prefix]

        # Read the file as a text file
        raw_df = self.spark.read.text(file_path)

        # Skip first row by dropping it
        raw_df = (
            raw_df.withColumn("row_num", F.monotonically_increasing_id())
            .filter(F.col("row_num") > 0)
            .drop("row_num")
        )

        # Define a UDF to parse the line
        parse_line_udf = udf(parse_function, schema)

        # Apply the UDF and select all fields
        parsed_df = raw_df.select(parse_line_udf(col("value")).alias("parsed")).select(
            "parsed.*"
        )

        # Cast the columns to the correct types as per the schema
        for field in schema.fields:
            parsed_df = parsed_df.withColumn(
                field.name, col(field.name).cast(field.dataType)
            )

        # Select only the columns specified in self.columns_to_select
        if file_prefix in self.columns_to_select:
            columns_to_select = self.columns_to_select[file_prefix]
            parsed_df = parsed_df.select(*columns_to_select)
        else:
            logger.error(f"Warning: No column selection specified for {file_prefix}")

        return parsed_df

    def process_and_join_files(self, filepaths: List[str]) -> DataFrame:
        logger.info(f"Starting to process {len(filepaths)} fnbo files.")
        # Group files by their file type
        file_groups = {}
        for file in filepaths:
            base_name = re.sub(r"_\d{3}_of_\d{3}\.txt$", ".txt", file)
            if base_name not in file_groups:
                file_groups[base_name] = []
            file_groups[base_name].append(file)
        logger.info("File groups:", file_groups)

        # Process files and create DataFrame dictionary
        df_dict = {}
        for base_name, files in file_groups.items():
            prefix = self.extract_file_prefix(base_name)
            if prefix is None:
                error_message = f"Found files with no matching prefix: {base_name}"
                raise ValueError(error_message)

            if prefix not in self.schemas:
                error_message = f"No schema found for prefix: {prefix}"
                raise ValueError(error_message)

            # Read and union all part files
            df = None
            for file in files:
                temp_df = self.process_file(file)
                # logger.info(f"temp_df for {file}")

                # print(temp_df.columns)

                if df is None:
                    df = temp_df
                else:
                    # logger.info(f"Unioning DataFrame for {file}")
                    df = df.union(temp_df)
            if df is not None:
                # Apply transformations
                logger.info(f"Prior to Applying transformations for prefix '{prefix}'")
                # df.show(2)
                if prefix in self.transformations:
                    df = self.transformations[prefix](df)
                    logger.info(f"Applied transformations for prefix '{prefix}'")
                    # df.show()
                else:
                    logger.warning(
                        f"Warning: No transformations specified for prefix '{prefix}'"
                    )

                df = clean_all_date_fields(df, ["account_open_date"])
                # Handle duplicates for each dataframe
                try:
                    # Determine which deduplication key to use based on available columns
                    dedupekey = None
                    if "account_open_date" in df.columns:
                        dedupekey = "account_open_date"
                    elif "arn" in df.columns:
                        dedupekey = "arn"
                
                    if dedupekey:
                        logger.info(f"Deduplicating using {dedupekey} for prefix '{prefix}'")
                        
                        if dedupekey == "account_open_date":
                            # Use deduplicate_by_timestamp for account_open_date
                            df_deduped = self.common_lib.deduplicate_by_timestamp(
                                df=df,
                                dedupekey=dedupekey,
                                timestamp_format= "yyyyMMdd",
                                validate_timestamps=True,
                                drop_invalid_timestamps=False,
                                preserve_case=True
                            )
                            df = df_deduped
                
                            # Drop account_open_date column for all prefixes except "cif"
                            if prefix != "cif":
                                # Get case-insensitive match for the dedupekey column
                                columns_lower = [col_name.lower() for col_name in df.columns]
                                if dedupekey.lower() in columns_lower:
                                    # Find the actual column name with original case
                                    actual_column = df.columns[columns_lower.index(dedupekey.lower())]
                                    logger.info(f"Dropping column '{actual_column}' for prefix '{prefix}'")
                                    df = df.drop(actual_column)
                        
                        else:  # dedupekey == "arn"
                            # For arn, just use simple deduplication
                            df = df.dropDuplicates(subset=["arn"])
                            logger.info("Performed simple deduplication using 'arn' column")
                
                        # Log deduplication statistics
                        initial_count = df.count()
                        final_count = df_deduped.count() if dedupekey == "account_open_date" else df.count()
                        records_removed = initial_count - final_count
                        
                        logger.info("Deduplication statistics:")
                        logger.info(f"- Initial record count: {initial_count}")
                        logger.info(f"- Final record count: {final_count}")
                        logger.info(f"- Duplicate records removed: {records_removed}")
                        
                        if records_removed > 0:
                            removal_percentage = (records_removed / initial_count) * 100
                            logger.info(f"- Percentage of records removed: {removal_percentage:.2f}%")
                
                    else:
                        logger.warning(
                            f"Neither account_open_date nor arn column found in DataFrame "
                            f"for prefix '{prefix}'. Available columns: {', '.join(df.columns)}. "
                            "Skipping deduplication."
                        )
                
                except Exception as e:
                    logger.error(
                        f"Error during deduplication process for prefix '{prefix}': {str(e)}"
                    )
                    raise
                

                # Add the processed DataFrame to the dictionary
                df_dict[prefix] = df
        join_mapping = {
            "cif": "arn",
            "naf": "arn",
            "amf": "arn",
        }
        joined_df = self.join_dataframes(df_dict, join_mapping, self.base_df_name)

        if joined_df is not None:
            logger.info("Join completed. Applying post-join transformations.")
            # Log a few zip codes before transformation
            # logger.info("Sample zip codes before transformation:")
            # joined_df.select("zip_code").show(5)
            # Add source_name column based on condition and trim zip_code to first 5 digits to exclude zip4
            joined_df = (joined_df
            .withColumn(
                "source_name",
                F.when(F.col("resort_area_code") == "UOR", F.lit("UOR_FNBO"))
                .when(F.col("resort_area_code") == "USH", F.lit("USH_FNBO"))
                .otherwise(None)
                )
                .withColumn(
                    "zip_code",
                    F.substring(F.col("zip_code"), 1, 5)
                )
            )
            # Log a few zip codes after transformation
            # logger.info("Sample zip codes after transformation:")
            # joined_df.select("zip_code").show(5)

            logger.info("Post-join transformations applied. Resulting schema:")
            joined_df.printSchema()
            # logger.info(f"Final DataFrame row count: {joined_df.count()}")
            return joined_df
        else:
            logger.error("No DataFrames to join")
            return None

    # Custom join function as source_customer_Id or arn has a CASE condition
    def join_dataframes(self, df_dict, join_mapping, base_df_name):
        # Start with CIF DataFrame
        cif_df = df_dict['cif'].alias('cif')
        naf_df = df_dict['naf'].alias('naf')
        amf_df = df_dict['amf'].alias('amf')

        # Log initial counts
        cif_count = cif_df.count()
        naf_count = naf_df.count()
        amf_count = amf_df.count()
        
        logger.info(f"Initial CIF count: {cif_count}")
        logger.info(f"Initial NAF count: {naf_count}")
        logger.info(f"Initial AMF count: {amf_count}")

        # First do inner join between CIF and NAF
        base_df = cif_df.join(
            naf_df,
            cif_df[f"cif.{join_mapping['cif']}"] == naf_df[f"naf.{join_mapping['naf']}"],
            "inner"
        ).drop(naf_df[join_mapping['naf']])

        # Log count after CIF-NAF join
        after_cif_naf_join_count = base_df.count()
        logger.info(f"\nAfter CIF-NAF inner join:")
        logger.info(f"Record count: {after_cif_naf_join_count}")

        # Then do left join with AMF
        base_df = base_df.join(
            amf_df,
            base_df[f"cif.{join_mapping['cif']}"] == amf_df[f"amf.{join_mapping['amf']}"],
            "left"
        ).drop(amf_df[join_mapping['amf']])

        # Log count after AMF left join
        final_count = base_df.count()
        logger.info(f"\nAfter AMF left join:")
        logger.info(f"Final record count: {final_count}")

        if final_count > after_cif_naf_join_count:
            logger.warning(
                f"AMF join resulted in {final_count - after_cif_naf_join_count} additional records"
            )

        # Select columns with the new ARN logic for source_customer_id
        columns_to_select = []
        has_amf_previous_record_identifier = False
        base_arn_column = None
        amf_arn_column = None

        # First pass to identify necessary columns

        # First pass to identify necessary columns
        for column in base_df.columns:
            if column.endswith(".arn"):
                if column.startswith("base."):
                    base_arn_column = column  # Store base.arn column name
                elif column.startswith("amf."):
                    amf_arn_column = column  # Store amf.arn column name
            elif column.endswith(".previous_record_identifier"):
                has_amf_previous_record_identifier = True

        # Add columns to select list with the conditional ARN logic
        for column in base_df.columns:
            if column.endswith(".arn"):
                if column.startswith("base."):
                    if has_amf_previous_record_identifier and amf_arn_column:
                        # Implement the CASE WHEN logic
                        columns_to_select.append(
                            F.when(
                                col(base_arn_column)
                                == col("amf.previous_record_identifier"),
                                col(amf_arn_column),
                            )
                            .otherwise(col(base_arn_column))
                            .alias("arn")  # Results in single 'arn' column
                        )
                    else:
                        columns_to_select.append(
                            col(column).alias("arn")
                        )  # for naf file single arn
            elif not column.endswith(".previous_record_identifier") and not (
                column.endswith(".arn") and not column.startswith("base.")
            ):
                columns_to_select.append(col(column))  # Includes all other columns

        final_df = base_df.select(*columns_to_select)

        # Final count check
        final_count = final_df.count()
        logger.info(f"\nFinal record count: {final_count}")

        return final_df

    def map_to_common_schema(self, source_df: DataFrame) -> DataFrame:
        # Define the mapping from source columns to target columns
        column_mapping = {
            "source_name": col("source_name"),
            "source_customer_id": col("arn"),
            "prefix": col("prefix"),
            "first_name": col("first_name"),
            "middle_name": col("middle_initial"),
            "last_name": col("last_name"),
            "generation_qualifier": lit(""),
            "gender": lit(""),
            "birth_date": lit(""),
            "address_line_1": col("address_line_1"),
            "address_line_2": col("address_line_2"),
            "city": col("city"),
            "state_code": col("state"),
            "postal_code": col("zip_code"),
            "country_code": lit(""),  # not availble in source
            "phone": col("phone"),
            "email_address": col("email"),
            "guest_id": lit(""),
            "record_created_timestamp": lit(""),
            "record_last_updated_timestamp": col(
                "account_open_date"
            ),  # Using account_open_date
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

    def run(self, filepaths: List[str], source) -> DataFrame:
        """
        Main method to run the entire data processing pipeline for fnbo files.

        Args:
            filepaths (List[str]): List of file paths to process.

        Returns:
            DataFrame: The final processed, joined, and mapped DataFrame.
        """
        try:
            # logger.info(f"Starting to process {len(filepaths)} fnbo files.")

            # Process and join files
            result_df = self.process_and_join_files(filepaths)

            if result_df is not None and result_df.count() > 0:
                logger.info("Processing and joining completed successfully.")
                logger.info(f"Joined DataFrame row count: {result_df.count()}")

                # Map to common schema
                output_df = self.map_to_common_schema(result_df)

                logger.info("Mapping to common schema completed.")
                logger.info(f"Final DataFrame row count: {output_df.count()}")
                logger.info("Final DataFrame schema:")
                output_df.printSchema()

                return output_df
            else:
                logger.error("No data to process or join.")
                return None
        except Exception as e:
            logger.exception(f"An error occurred during fnbo processing: {str(e)}")
            import traceback

            print(traceback.format_exc())
            raise
