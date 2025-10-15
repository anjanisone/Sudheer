from pyspark.sql.functions import col, lower, trim
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from pyspark.sql.functions import upper, lower, trim, length, when, col, max, row_number
from pyspark.sql.functions import to_timestamp, date_format, coalesce, lit
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import (
    col,
    when,
    coalesce,
    to_timestamp,
    date_format,
    from_utc_timestamp,
    to_utc_timestamp,
    unix_timestamp,
    expr,
    regexp_replace,
    to_timestamp,
    date_format,
)


from typing import List, Optional, Dict
from pyspark.sql.functions import from_utc_timestamp, to_timestamp, col, when
import logging, re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CommonLibrary:
    def __init__(self):
        self.common_schema = [
            "source_name",
            "source_customer_id",
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
            "guest_id",
            "record_created_timestamp",
            "record_last_updated_timestamp",
        ]

        self.max_retries = 3
        self.timeout = 30

        self.renamed_cols = {
            "guest_id": "rules_guest_id",
            "source_name": "source_name",
            "source_customer_id": "source_customer_id",
            "record_created_timestamp": "record_created_timestamp",
            "record_last_updated_timestamp": "record_last_updated_timestamp",
        }

    def trim_upper(self, df: DataFrame) -> DataFrame:
        for column in df.columns:
            df = df.withColumn(column, upper(trim(col(column))))
        return df

    def trim_lower(self, df: DataFrame) -> DataFrame:
        for column in df.columns:
            df = df.withColumn(column, lower(trim(col(column))))
        return df

    def validate_email(
        self, df: DataFrame, email_column: str, max_length: int = 128
    ) -> DataFrame:
        return df.withColumn(
            email_column,
            when(
                length(lower(trim(col(email_column)))) <= max_length,
                lower(trim(col(email_column))),
            ).otherwise(None),
        )

    def get_col_prefix(self, col_name):
        """
        Function to determine the new column prefix
        """
        if col_name in self.renamed_cols:
            return self.renamed_cols[col_name]
        elif col_name == "pass_thru_data":
            return col_name
        else:
            return f"ml_{col_name}"

    def rename_columns(self, df):
        """
        Utility to rename columns with ml_ or rules_ prefix
        """
        for old_name in df.columns:
            new_name = self.get_col_prefix(old_name)
            df = df.withColumnRenamed(old_name, new_name)
        return df

    @staticmethod
    def clean_all_date_fields(df, columns):
        """
        Cleans and standardizes only the specified date/timestamp columns to ISO 8601.
        - Trims & normalizes raw strings
        - Replaces placeholders with null
        - Parses numeric and month-name formats
        - Nullifies out-of-range years (<1900 or > current year)
        - Formats to ISO:
            * Date-like cols  -> 'yyyy-MM-dd'
            * Timestamp/time  -> 'yyyy-MM-ddTHH:mm:ssZ'
        """
        from datetime import datetime
        from pyspark.sql import functions as F

        now = datetime.now()
        current_year = now.year
        min_year = 1900
        invalid_values = ["null", "none", "n/a", "missing", "not null", "", "na"]

        # Allow single string or list of columns
        if isinstance(columns, str):
            columns = [columns]

        if columns:
            for c in columns:
                if c not in df.columns:
                    continue  # skip missing columns gracefully

                # Trim & replace invalid placeholders
                df = df.withColumn(c, F.trim(F.col(c)))
                df = df.withColumn(c, F.when(F.col(c).isin(invalid_values), None).otherwise(F.col(c)))

                # Parse multiple date/time formats (supports month names too)
                parsed = F.coalesce(
                        F.to_date(F.col(c), "yyyy-MM-dd"),
                        F.to_date(F.col(c), "yyyy/MM/dd"),
                        F.to_date(F.col(c), "MM/dd/yyyy"),
                        F.to_date(F.col(c), "dd-MM-yyyy"),
                        F.to_date(F.col(c), "dd/MM/yyyy"),
                        F.to_date(F.col(c), "yyyyMMdd"),
                        F.to_date(F.col(c), "MMM dd, yyyy"),
                        F.to_date(F.col(c), "MMMM dd, yyyy"),
                        F.to_date(F.col(c), "dd-MMM-yyyy"),
                        F.to_date(F.col(c), "dd-MMMM-yyyy")
                    )
                df = df.withColumn(c, parsed)

                # Nullify out-of-range or invalid years
                df = df.withColumn(
                    c,
                    F.when(
                        (F.year(F.col(c)) < min_year) | (F.year(F.col(c)) > current_year),
                        None
                    ).otherwise(F.col(c))
                )

                # Convert to ISO 8601 format
                iso_format = "yyyy-MM-dd'T'HH:mm:ss'Z'" if ("timestamp" in c.lower() or "time" in c.lower()) else "yyyy-MM-dd"
                df = df.withColumn(c, F.date_format(F.col(c), iso_format))

        return df

    def convert_timestamp_to_utc(
                self,
                df: DataFrame,
                created_timestamp_column: str = "record_created_timestamp",
                updated_timestamp_column: str = "record_last_updated_timestamp",
                source: str = None,
            ) -> DataFrame:
                """
                Convert timestamps to UTC based on source system timezone
                Args:
                    df: Input DataFrame
                    created_timestamp_column: Column containing the creation timestamp
                    updated_timestamp_column: Column containing the last updated timestamp
                    source: Source system name
                """
                timezone_mapping = {
                    "CST": ["UDX_FNBO", "USH_TRISEPT"],
                    "EST": [
                        "UDX_ADHOC",
                        "UOR_COLORVISION",
                        "UOR_COMMERCE_CART",
                        "UOR_GALAXY",
                        "UOR_LOEWS",
                        "UOR_WIFI",
                    ],
                    "PST": ["USH_DENALI", "USH_GALAXY", "USH_WIFI"],
                    "UTC": ["UDX_TRISEPT"],
                }
        
                # Get timezone for the source
                source_timezone = "UTC"  # Default to UTC
                for tz, sources in timezone_mapping.items():
                    if source in sources:
                        source_timezone = tz
                        break
        
                logger.info(f"Using timezone {source_timezone} for source {source}")
        
                # Function to apply timestamp conversion logic
                def apply_timestamp_conversion(timestamp_column):
                    # First, trim any extra spaces
                    df_temp = df.withColumn(timestamp_column, trim(col(timestamp_column)))
        
                    return when(
                        lit(source) == "UDX_TRISEPT",
                        # Format: 2024-12-16 03:54:30.005913, no space b/w day and hour is handled in triseptclass
                        to_utc_timestamp(
                            to_timestamp(
                                col(timestamp_column), "yyyy-MM-dd HH:mm:ss.SSSSSS"
                            ),
                            source_timezone
                        )
                    ).when(
                        lit(source) == "UOR_LOEWS",
                        # Format: 12/14/2024 11:09:33 PM
                        to_utc_timestamp(
                            to_timestamp(col(timestamp_column), "MM/dd/yyyy hh:mm:ss a"),
                            source_timezone
                        )
                    ).when(
                        lit(source).isin("USH_WIFI", "UOR_WIFI"),
                        # Format: 2024-12-15 19:17:45
                        to_utc_timestamp(
                            to_timestamp(col(timestamp_column), "yyyy-MM-dd HH:mm:ss"),
                            source_timezone
                        )
                    ).when(
                        lit(source).isin("USH_DENALI", "UOR_GALAXY", "USH_GALAXY"),
                        # Format: 2024-12-13 22:19:23.617
                        to_utc_timestamp(
                            to_timestamp(col(timestamp_column), "yyyy-MM-dd HH:mm:ss.SSS"),
                            source_timezone
                        )
                    ).when(
                                lit(source) == "UOR_COMMERCE_CART",
                                # Format: 2024-10-24T17:47:18.726-04:00
                                to_timestamp(col(timestamp_column), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                            
                    ).when(
                        lit(source) == "UOR_COLORVISION",
                        to_utc_timestamp(
                            coalesce(
                                # Format 1: 12-hour format with seconds (e.g., "01/28/2025 09:41:40 AM" or "1/2/2025 9:05:01 AM")
                                to_timestamp(col(timestamp_column), "M/d/yyyy h:mm:ss a"),
                                
                                # Format 2: 12-hour format without seconds (e.g., "01/28/2025 09:41 AM" or "1/2/2025 9:05 AM")
                                to_timestamp(col(timestamp_column), "M/d/yyyy h:mm a"),
                                
                                # Format 3: 24-hour format (e.g., "01/29/2025 21:41" or "1/2/2025 21:41")
                                to_timestamp(col(timestamp_column), "M/d/yyyy HH:mm")
                            ),
                            source_timezone
                        )
                    ).when(
                        lit(source) == "UDX_FNBO",
                        # Format: 20241204
                        to_utc_timestamp(
                            to_timestamp(col(timestamp_column), "yyyyMMdd"),
                            source_timezone
                        )
                    ).when(
                        lit(source) == "UDX_ADHOC",
                        # Format: 9/9/2024
                        to_utc_timestamp(
                            coalesce(
                                to_timestamp(col(timestamp_column), "yyyy-MM-dd HH:mm:ss.SSS"),
                                to_timestamp(col(timestamp_column), "M/d/yyyy"),
                                to_timestamp(col(timestamp_column), "yyyy-MM-dd")
                            ),
                                source_timezone
                        )
                    ).when(
                        lit(source) == "USH_TRISEPT",
                        # Format: 2024-10-15 08:02:00
                        to_utc_timestamp(
                            to_timestamp(col(timestamp_column), "yyyy-MM-dd HH:mm:ss"),
                            source_timezone
                        )
                    ).otherwise(
                        # Default fallback for any unhandled formats
                        to_utc_timestamp(
                            coalesce(
                                to_timestamp(col(timestamp_column), "yyyy-MM-dd HH:mm:ss.SSS"),
                                to_timestamp(col(timestamp_column), "yyyy-MM-dd HH:mm:ss"),
                                to_timestamp(col(timestamp_column), "yyyy-MM-dd")
                            ),
                            source_timezone
                        )
                    )
    
                # Apply conversion to both timestamp columns
                df = df.withColumn(
                    "parsed_created_timestamp",
                    apply_timestamp_conversion(created_timestamp_column)
                ).withColumn(
                    "parsed_updated_timestamp",
                    apply_timestamp_conversion(updated_timestamp_column)
                )
    
                # Format both timestamps
                df = df.withColumn(
                    created_timestamp_column,
                    date_format(
                        col("parsed_created_timestamp"),
                        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                    )
                ).withColumn(
                    updated_timestamp_column,
                    date_format(
                        col("parsed_updated_timestamp"),
                        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                    )
                )
    
                # Drop temporary columns
                df = df.drop("parsed_created_timestamp", "parsed_updated_timestamp")
    
                return df
    
   
    def deduplicate_by_timestamp(
            self,
            df: DataFrame, 
            dedupekey: str, 
            timestamp_format: str,
            validate_timestamps: bool = True,
            drop_invalid_timestamps: bool = False,
            preserve_case: bool = True
        ) -> DataFrame:
            """
            Enhanced version with timestamp validation, case-insensitive handling and case preservation
            
            Args:
                df (DataFrame): Input DataFrame
                dedupekey (str): Column name containing timestamp to be used for deduplication
                timestamp_format (str): Format of the timestamp
                validate_timestamps (bool): Whether to validate timestamp conversion
                drop_invalid_timestamps (bool): Whether to drop rows with invalid timestamps
                preserve_case (bool): Whether to preserve original column case
            """
            try:
                # Store original columns for case preservation
                original_columns = df.columns
                
                # Convert all column names to lowercase and create a new DataFrame
                lowercase_columns = [col.lower() for col in df.columns]
                column_mapping = dict(zip(df.columns, lowercase_columns))
                
                # Create DataFrame with lowercase columns
                df_lower = df.select([
                    col(c).alias(column_mapping[c]) 
                    for c in df.columns
                ])
                
                # Convert dedupekey to lowercase for comparison
                dedupekey_lower = dedupekey.lower()
                
                if dedupekey_lower not in lowercase_columns:
                    raise ValueError(
                        f"Column {dedupekey} (case-insensitive) not found in DataFrame. "
                        f"Available columns: {', '.join(original_columns)}"
                    )
                
                # Log initial count
                initial_count = df_lower.count()
                logger.info(f"Initial record count: {initial_count}")
    
                # Show duplicate records before deduplication
                logger.info("DUPLICATE records before deduplication (showing counts):")
                duplicate_counts = (df_lower.groupBy(df_lower.columns)
                                        .count()
                                        .filter("count > 1")
                                        .orderBy("count", ascending=False))
                
                duplicate_count = duplicate_counts.count()
                logger.info(f"Found {duplicate_count} groups of duplicates")
                
                if duplicate_count > 0:
                    logger.info("Sample of duplicate records:")
                    # duplicate_counts.limit(5).show(truncate=False)
    
                # Create timestamp column
                ts_column = f"{dedupekey_lower}_ts"
                df_with_ts = df_lower.withColumn(
                    ts_column, 
                    to_timestamp(col(dedupekey_lower), timestamp_format)
                )
    
                # Validate timestamps if requested
                if validate_timestamps:
                    invalid_count = df_with_ts.filter(col(ts_column).isNull()).count()
                    if invalid_count > 0:
                        message = f"Found {invalid_count} rows with invalid timestamps"
                        if drop_invalid_timestamps:
                            logger.warning(f"{message}. These rows will be dropped.")
                            df_with_ts = df_with_ts.filter(col(ts_column).isNotNull())
                        else:
                            logger.warning(f"{message}. These rows will be kept.")
    
                # Create deduplication column list using comprehension
                columns_for_deduplication = [
                    col if col != dedupekey_lower else ts_column 
                    for col in df_with_ts.columns 
                    if col != ts_column
                ]
    
                # First deduplication - removes exact duplicates
                logger.info("Performing first deduplication step - removing exact duplicates...")
                deduplicated_df = df_with_ts.dropDuplicates(subset=columns_for_deduplication)
    
                # Second deduplication - keep latest by timestamp
                logger.info("Performing second deduplication step - keeping latest records...")
                final_df = (deduplicated_df
                        .orderBy(col(ts_column).desc())
                        .dropDuplicates(subset=[
                            col for col in columns_for_deduplication 
                            if col != ts_column
                        ]))
    
                # Drop temporary timestamp column
                result_df = final_df.drop(ts_column)
    
                # Restore original column case if requested
                if preserve_case:
                    logger.info("Restoring original column case...")
                    # Create reverse mapping from lowercase to original case
                    reverse_mapping = dict(zip(lowercase_columns, original_columns))
                    
                    # Verify all current columns have a mapping
                    missing_columns = [
                        col for col in result_df.columns 
                        if col not in reverse_mapping
                    ]
                    if missing_columns:
                        raise ValueError(
                            f"Cannot find original case for columns: {missing_columns}"
                        )
                    
                    # Apply reverse mapping to restore original case
                    result_df = result_df.select([
                        col(c).alias(reverse_mapping[c]) 
                        for c in result_df.columns
                    ])
                    logger.info("Original column case restored successfully")
    
                # Log final counts and statistics
                final_count = result_df.count()
                records_removed = initial_count - final_count
                logger.info(f"Deduplication complete:")
                logger.info(f"- Initial record count: {initial_count}")
                logger.info(f"- Final record count: {final_count}")
                logger.info(f"- Duplicate records removed: {records_removed}")
                
                if records_removed > 0:
                    removal_percentage = (records_removed / initial_count) * 100
                    logger.info(f"- Percentage of records removed: {removal_percentage:.2f}%")
    
                return result_df
    
            except Exception as e:
                logger.error(f"Error during deduplication: {str(e)}")
                raise
    

# Creating a singleton instance for ease of use
common_lib = CommonLibrary()

