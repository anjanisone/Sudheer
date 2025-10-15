import sys
import json, ast
from typing import List
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import regexp_extract, col, input_file_name, collect_list
from pyspark.sql.functions import upper, lower, trim, length, when, col, max, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, col, coalesce
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import input_file_name, regexp_extract, col
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, expr
from pyspark.sql.column import Column
from pyspark.sql.functions import count, col
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat, substring, lpad, lit, row_number, lower, trim
from pyspark.sql.window import Window


import logging, re
from common_library import common_lib, clean_all_date_fields

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Pre-Prod data in input files is in pretty bad shape with spaces and blanks, so applied some guard rails.
class LoewsDataProcessor:
    def __init__(self, glue_context):
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.common_lib = common_lib
        self.dedupekey = "UPDATE_DATE"
        self.base_df_name = "uo_loews_reservation_name"

        # Define schemas, columns, and other configurations
        self.reservation_name_schema = [
            "RESORT",
            "RESV_NAME_ID",
            "NAME_ID",
            "NAME_USAGE_TYPE",
            "CONTACT_NAME_ID",
            "INSERT_DATE",
            "INSERT_USER",
            "UPDATE_USER",
            "UPDATE_DATE",
            "RESV_STATUS",
            "COMMISSION_CODE",
            "ADDRESS_ID",
            "PHONE_ID",
            "FAX_YN",
            "MAIL_YN",
            "PRINT_RATE_YN",
            "REPORT_ID",
            "RESV_NO",
            "CONFIRMATION_NO",
            "BEGIN_DATE",
            "END_DATE",
            "FAX_ID",
            "EMAIL_ID",
            "EMAIL_YN",
            "CONSUMER_YN",
            "CREDIT_CARD_ID",
            "FINANCIALLY_RESPONSIBLE_YN",
            "PAYMENT_METHOD",
            "INTERMEDIARY_YN",
            "POSTING_ALLOWED_YN",
            "DISPLAY_COLOR",
            "ACTUAL_CHECK_IN_DATE",
            "TRUNC_ACTUAL_CHECK_IN_DATE",
            "ACTUAL_CHECK_OUT_DATE",
            "TRUNC_ACTUAL_CHECK_OUT_DATE",
            "CREDIT_LIMIT",
            "AUTHORIZED_BY",
            "PARENT_RESV_NAME_ID",
            "CANCELLATION_NO",
            "CANCELLATION_REASON_CODE",
            "CANCELLATION_REASON_DESC",
            "ARRIVAL_TRANSPORT_TYPE",
            "ARRIVAL_STATION_CODE",
            "ARRIVAL_CARRIER_CODE",
            "ARRIVAL_TRANSPORT_CODE",
            "ARRIVAL_DATE_TIME",
            "ARRIVAL_ESTIMATE_TIME",
            "ARRIVAL_TRANPORTATION_YN",
            "ARRIVAL_COMMENTS",
            "DEPARTURE_TRANSPORT_TYPE",
            "DEPARTURE_STATION_CODE",
            "DEPARTURE_CARRIER_CODE",
            "DEPARTURE_TRANSPORT_CODE",
            "DEPARTURE_DATE_TIME",
            "DEPARTURE_ESTIMATE_TIME",
            "DEPARTURE_TRANSPORTATION_YN",
            "DEPARTURE_COMMENTS",
            "CANCELLATION_DATE",
            "GUARANTEE_CODE",
            "WL_REASON_DESCRIPTION",
            "WL_REASON_CODE",
            "WL_PRIORITY",
            "DO_NOT_MOVE_ROOM",
            "EXTERNAL_REFERENCE",
            "PARTY_CODE",
            "WALKIN_YN",
            "ORIGINAL_END_DATE",
            "APPROVAL_AMOUNT_CALC_METHOD",
            "AMOUNT_PERCENT",
            "NAME_TAX_TYPE",
            "TAX_EXEMPT_NO",
            "ROOM_FEATURES",
            "WL_TELEPHONE_NO",
            "VIDEO_CHECKOUT_YN",
            "DISCOUNT_AMT",
            "DISCOUNT_PRCNT",
            "DISCOUNT_REASON_CODE",
            "COMMISSION_PAID",
            "COMMISSION_HOLD_CODE",
            "TRUNC_BEGIN_DATE",
            "TRUNC_END_DATE",
            "SGUEST_NAME",
            "MEMBERSHIP_ID",
            "UDFC01",
            "UDFC02",
            "UDFC03",
            "UDFC04",
            "UDFC05",
            "UDFC06",
            "UDFC07",
            "UDFC08",
            "UDFC09",
            "UDFC10",
            "UDFC11",
            "UDFC12",
            "UDFC13",
            "UDFC14",
            "UDFC15",
            "UDFC16",
            "UDFC17",
            "UDFC18",
            "UDFC19",
            "UDFC20",
            "UDFC21",
            "UDFC22",
            "UDFC23",
            "UDFC24",
            "UDFC25",
            "UDFC26",
            "UDFC27",
            "UDFC28",
            "UDFC29",
            "UDFC30",
            "UDFC31",
            "UDFC32",
            "UDFC33",
            "UDFC34",
            "UDFC35",
            "UDFC36",
            "UDFC37",
            "UDFC38",
            "UDFC39",
            "UDFC40",
            "UDFN01",
            "UDFN02",
            "UDFN03",
            "UDFN04",
            "UDFN05",
            "UDFN06",
            "UDFN07",
            "UDFN08",
            "UDFN09",
            "UDFN10",
            "UDFN11",
            "UDFN12",
            "UDFN13",
            "UDFN14",
            "UDFN15",
            "UDFN16",
            "UDFN17",
            "UDFN18",
            "UDFN19",
            "UDFN20",
            "UDFN21",
            "UDFN22",
            "UDFN23",
            "UDFN24",
            "UDFN25",
            "UDFN26",
            "UDFN27",
            "UDFN28",
            "UDFN29",
            "UDFN30",
            "UDFN31",
            "UDFN32",
            "UDFN33",
            "UDFN34",
            "UDFN35",
            "UDFN36",
            "UDFN37",
            "UDFN38",
            "UDFN39",
            "UDFN40",
            "UDFD01",
            "UDFD02",
            "UDFD03",
            "UDFD04",
            "UDFD05",
            "UDFD06",
            "UDFD07",
            "UDFD08",
            "UDFD09",
            "UDFD10",
            "UDFD11",
            "UDFD12",
            "UDFD13",
            "UDFD14",
            "UDFD15",
            "UDFD16",
            "UDFD17",
            "UDFD18",
            "UDFD19",
            "UDFD20",
            "INSERT_ACTION_INSTANCE_ID",
            "DML_SEQ_NO",
            "BUSINESS_DATE_CREATED",
            "TURNDOWN_YN",
            "ROOM_INSTRUCTIONS",
            "ROOM_SERVICE_TIME",
            "EVENT_ID",
            "REVENUE_TYPE_CODE",
            "HURDLE",
            "HURDLE_OVERRIDE",
            "RATEABLE_VALUE",
            "RESTRICTION_OVERRIDE",
            "YIELDABLE_YN",
            "SGUEST_FIRSTNAME",
            "GUEST_LAST_NAME",
            "GUEST_FIRST_NAME",
            "GUEST_LAST_NAME_SDX",
            "GUEST_FIRST_NAME_SDX",
            "CHANNEL",
            "SHARE_SEQ_NO",
            "GUEST_SIGNATURE",
            "EXTENSION_ID",
            "RESV_CONTACT_ID",
            "BILLING_CONTACT_ID",
            "RES_INSERT_SOURCE",
            "RES_INSERT_SOURCE_TYPE",
            "MASTER_SHARE",
            "REGISTRATION_CARD_NO",
            "TIAD",
            "PURPOSE_OF_STAY",
            "REINSTATE_DATE",
            "PURGE_DATE",
            "LAST_SETTLE_DATE",
            "LAST_PERIODIC_FOLIO_DATE",
            "PERIODIC_FOLIO_FREQ",
            "CONFIRMATION_LEG_NO",
            "GUEST_STATUS",
            "GUEST_TYPE",
            "CHECKIN_DURATION",
            "AUTHORIZER_ID",
            "LAST_ONLINE_PRINT_SEQ",
            "ENTRY_POINT",
            "ENTRY_DATE",
            "FOLIO_TEXT1",
            "FOLIO_TEXT2",
            "PSEUDO_MEM_TYPE",
            "PSEUDO_MEM_TOTAL_POINTS",
            "COMP_TYPE_CODE",
            "UNI_CARD_ID",
            "EXP_CHECKINRES_ID",
            "ORIGINAL_BEGIN_DATE",
            "OWNER_FF_FLAG",
            "COMMISSION_PAYOUT_TO",
            "PRE_CHARGING_YN",
            "POST_CHARGING_YN",
            "POST_CO_FLAG",
            "FOLIO_CLOSE_DATE",
            "SCHEDULE_CHECKOUT_YN",
            "CUSTOM_REFERENCE",
            "GUARANTEE_CODE_PRE_CI",
            "AWARD_MEMBERSHIP_ID",
            "ESIGNED_REG_CARD_NAME",
            "STATISTICAL_ROOM_TYPE",
            "STATISTICAL_RATE_TIER",
            "YM_CODE",
            "KEY_VALID_UNTIL",
            "KEY_PIN_CODE",
            "PRE_REGISTERED_YN",
            "TAX_REGISTRATION_NO",
            "VISA_NUMBER",
            "VISA_ISSUE_DATE",
            "VISA_EXPIRATION_DATE",
            "TAX_NO_OF_STAYS",
            "ASB_PRORATED_YN",
            "AUTO_SETTLE_DAYS",
            "AUTO_SETTLE_YN",
            "SPLIT_FROM_RESV_NAME_ID",
            "NEXT_DESTINATION",
            "DATE_OF_ARRIVAL_IN_COUNTRY",
            "PRE_ARR_REVIEWED_DT",
            "PRE_ARR_REVIEWED_USER",
            "BONUS_CHECK_ID",
            "DIRECT_BILL_VERY_RESPONSE",
            "ADDRESSEE_NAME_ID",
            "SUPER_SEARCH_INDEX_TEXT",
            "AUTO_CHECKIN_YN",
            "EMAIL_FOLIO_YN",
            "EMAIL_ADDRESS",
        ]
        self.name_schema = [
            "NAME_ID",
            "NAME_CODE",
            "INSERT_USER",
            "INSERT_DATE",
            "UPDATE_USER",
            "UPDATE_DATE",
            "PRIMARY_NAME_ID",
            "REPEAT_GUEST_ID",
            "MAIL_LIST",
            "MAIL_TYPE",
            "FOLLOW_ON",
            "BUSINESS_TITLE",
            "INACTIVE_DATE",
            "ARC_UPDATE_DATE",
            "UPDATE_FAX_DATE",
            "BIRTH_DATE",
            "COLLECTION_USER_ID",
            "COMPANY",
            "SOUND_EX_COMPANY",
            "LEGAL_COMPANY",
            "FIRST",
            "MIDDLE",
            "LAST",
            "NICKNAME",
            "TITLE",
            "SOUND_EX_LAST",
            "EXTERNAL_REFERENCE_REQU",
            "VIP_STATUS",
            "VIP_AUTHORIZATION",
            "BILLING_PROFILE_CODE",
            "RATE_STRUCTURE",
            "NAME_COMMENT",
            "TOUR_OPERATOR_TYPE",
            "REGION",
            "TYPE_OF_1099",
            "TAX1_NO",
            "COMPANY_NAME_ID",
            "EXTERNAL_REFERENCE_REQUIRED",
            "VENDOR_ID",
            "VENDOR_SITE_ID",
            "ARC_OFFICE_TYPE",
            "TAX2_NO",
            "ARC_MAIL_FLAG",
            "NAME2",
            "NAME3",
            "SALESREP",
            "TRACECODE",
            "GEOGRAPHIC_REGION",
            "GUEST_CLASSIFICATION",
            "PRIMARY_ADDRESS_ID",
            "PRIMARY_PHONE_ID",
            "TAX_EXEMPT_STATUS",
            "GDS_NAME",
            "GDS_TRANSACTION_NO",
            "NATIONALITY",
            "LANGUAGE",
            "SALUTATION",
            "PASSPORT",
            "HISTORY_YN",
            "RESV_CONTACT",
            "CONTRACT_NO",
            "CONTRACT_RECV_DATE",
            "ACCT_CONTACT",
            "PRIORITY",
            "INDUSTRY_CODE",
            "ROOMS_POTENTIAL",
            "COMPETITION_CODE",
            "SCOPE",
            "SCOPE_CITY",
            "TERRITORY",
            "ACTIONCODE",
            "ACTIVE_YN",
            "MASTER_ACCOUNT_YN",
            "NAME_TYPE",
            "SNAME",
            "NAME_TAX_TYPE",
            "SFIRST",
            "AR_NO",
            "AVAILABILITY_OVERRIDE",
            "BILLING_CODE",
            "CASH_BL_IND",
            "BL_MSG",
            "CURRENCY_CODE",
            "COMMISSION_CODE",
            "HOLD_CODE",
            "INTEREST",
            "SUMM_REF_CC",
            "IATA_COMP_TYPE",
            "SREP_CODE",
            "ACCOUNTSOURCE",
            "MARKETS",
            "PRODUCT_INTEREST",
            "KEYWORD",
            "LETTER_GREETING",
            "INFLUENCE",
            "DEPT_ID",
            "DEPARTMENT",
            "CONTACT_YN",
            "ACCOUNT_TYPE",
            "DOWNLOAD_RESORT",
            "DOWNLOAD_SREP",
            "DOWNLOAD_DATE",
            "UPLOAD_DATE",
            "LAPTOP_CHANGE",
            "CRS_NAMEID",
            "COMM_PAY_CENTRAL",
            "CC_PROFILE_YN",
            "GENDER",
            "BIRTH_PLACE",
            "BIRTH_COUNTRY",
            "PROFESSION",
            "ID_TYPE",
            "ID_NUMBER",
            "ID_DATE",
            "ID_PLACE",
            "ID_COUNTRY",
            "UDFC01",
            "UDFC02",
            "UDFC03",
            "UDFC04",
            "UDFC05",
            "UDFC06",
            "UDFC07",
            "UDFC08",
            "UDFC09",
            "UDFC10",
            "UDFC11",
            "UDFC12",
            "UDFC13",
            "UDFC14",
            "UDFC15",
            "UDFC16",
            "UDFC17",
            "UDFC18",
            "UDFC19",
            "UDFC20",
            "UDFC21",
            "UDFC22",
            "UDFC23",
            "UDFC24",
            "UDFC25",
            "UDFC26",
            "UDFC27",
            "UDFC28",
            "UDFC29",
            "UDFC30",
            "UDFC31",
            "UDFC32",
            "UDFC33",
            "UDFC34",
            "UDFC35",
            "UDFC36",
            "UDFC37",
            "UDFC38",
            "UDFC39",
            "UDFC40",
            "UDFN01",
            "UDFN02",
            "UDFN03",
            "UDFN04",
            "UDFN05",
            "UDFN06",
            "UDFN07",
            "UDFN08",
            "UDFN09",
            "UDFN10",
            "UDFN11",
            "UDFN12",
            "UDFN13",
            "UDFN14",
            "UDFN15",
            "UDFN16",
            "UDFN17",
            "UDFN18",
            "UDFN19",
            "UDFN20",
            "UDFN21",
            "UDFN22",
            "UDFN23",
            "UDFN24",
            "UDFN25",
            "UDFN26",
            "UDFN27",
            "UDFN28",
            "UDFN29",
            "UDFN30",
            "UDFN31",
            "UDFN32",
            "UDFN33",
            "UDFN34",
            "UDFN35",
            "UDFN36",
            "UDFN37",
            "UDFN38",
            "UDFN39",
            "UDFN40",
            "UDFD01",
            "UDFD02",
            "UDFD03",
            "UDFD04",
            "UDFD05",
            "UDFD06",
            "UDFD07",
            "UDFD08",
            "UDFD09",
            "UDFD10",
            "UDFD11",
            "UDFD12",
            "UDFD13",
            "UDFD14",
            "UDFD15",
            "UDFD16",
            "UDFD17",
            "UDFD18",
            "UDFD19",
            "UDFD20",
            "PAYMENT_DUE_DAYS",
            "SUFFIX",
            "EXTERNAL_ID",
            "GUEST_PRIV_YN",
            "EMAIL_YN",
            "MAIL_YN",
            "INDEX_NAME",
            "XLAST_NAME",
            "XFIRST_NAME",
            "XCOMPANY_NAME",
            "XTITLE",
            "XSALUTATION",
            "SXNAME",
            "SXFIRST_NAME",
            "LAST_UPDATED_RESORT",
            "ENVELOPE_GREETING",
            "XENVELOPE_GREETING",
            "DIRECT_BILL_BATCH_TYPE",
            "RESORT_REGISTERED",
            "TAX_OFFICE",
            "TAX_TYPE",
            "TAX_CATEGORY",
            "PREFERRED_ROOM_NO",
            "PHONE_YN",
            "SMS_YN",
            "PROTECTED",
            "XLANGUAGE",
            "MARKET_RESEARCH_YN",
            "THIRD_PARTY_YN",
            "AUTOENROLL_MEMBER_YN",
            "CHAIN_CODE",
            "CREDIT_RATING",
            "TITLE_SUFFIX",
            "COMPANY_GROUP_ID",
            "INACTIVE_REASON",
            "IATA_CONSORTIA",
            "INCLUDE_IN_1099_YN",
            "PSUEDO_PROFILE_YN",
            "PROFILE_PRIVACY_FLG",
            "REPLACE_ADDRESS",
            "ALIEN_REGISTRATION_NO",
            "IMMIGRATION_STATUS",
            "VISA_VALIDITY_TYPE",
            "ID_DOCUMENT_ATTACH_ID",
        ]
        self.address_name_schema = [
            "ADDRESS_ID",
            "NAME_ID",
            "ADDRESS_TYPE",
            "INSERT_DATE",
            "INSERT_USER",
            "UPDATE_DATE",
            "UPDATE_USER",
            "BEGIN_DATE",
            "END_DATE",
            "ADDRESS1",
            "ADDRESS2",
            "ADDRESS3",
            "ADDRESS4",
            "CITY",
            "COUNTRY",
            "PROVINCE",
            "STATE",
            "ZIP_CODE",
            "INACTIVE_DATE",
            "PRIMARY_YN",
            "FOREIGN_COUNTRY",
            "IN_CARE_OF",
            "CITY_EXT",
            "LAPTOP_CHANGE",
            "LANGUAGE_CODE",
            "CLEANSED_STATUS",
            "CLEANSED_DATETIME",
            "CLEANSED_ERRORMSG",
            "CLEANSED_VALIDATIONSTATUS",
            "CLEANSED_MATCHSTATUS",
            "BARCODE",
            "LAST_UPDATED_RESORT",
        ]
        self.phone_name_schema = [
            "PHONE_ID",
            "NAME_ID",
            "PHONE_TYPE",
            "PHONE_ROLE",
            "PHONE_NUMBER",
            "INSERT_DATE",
            "INSERT_USER",
            "UPDATE_DATE",
            "UPDATE_USER",
            "INACTIVE_DATE",
            "END_DATE",
            "BEGIN_DATE",
            "ADDRESS_ID",
            "PRIMARY_YN",
            "DISPLAY_SEQ",
            "LAPTOP_CHANGE",
            "INDEX_PHONE",
            "EXTENSION",
            "EMAIL_FORMAT",
            "SHARE_EMAIL_YN",
            "DEFAULT_CONFIRMATION_YN",
            "EMAIL_LANGUAGE",
        ]
        # Define file prefixes as named variables

        # Define file prefixes as named variables
        self.PREFIX_RESERVATION = "uo_loews_reservation_name"
        self.PREFIX_NAME = "uo_loews_name"
        self.PREFIX_ADDRESS = "uo_loews_name_address"
        self.PREFIX_PHONE = "uo_loews_name_phone"

        # Define the list of file prefixes in the order of file_mapping_info doc
        self.file_prefixes = [
            self.PREFIX_RESERVATION,
            self.PREFIX_NAME,
            self.PREFIX_ADDRESS,
            self.PREFIX_PHONE,
        ]

        # Use the named prefix variables to define other configurations
        self.schemas = {
            self.PREFIX_RESERVATION: self.reservation_name_schema,
            self.PREFIX_NAME: self.name_schema,
            self.PREFIX_ADDRESS: self.address_name_schema,
            self.PREFIX_PHONE: self.phone_name_schema,
        }

        self.output_cols = self.common_lib.common_schema

        self.columns_to_select = {
            self.PREFIX_RESERVATION: [
                "RESV_NAME_ID",
                "NAME_ID",
                "INSERT_DATE",
                "UPDATE_DATE",
            ],
            self.PREFIX_NAME: [
                "BIRTH_DATE",
                "FIRST",
                "MIDDLE",
                "LAST",
                "TITLE",
                "NAME_ID",
                "UPDATE_DATE",
            ],
            self.PREFIX_ADDRESS: [
                "ADDRESS1",
                "ADDRESS2",
                "CITY",
                "COUNTRY",
                "STATE",
                "ZIP_CODE",
                "NAME_ID",
                "UPDATE_DATE",
                "PRIMARY_YN"
            ],
            self.PREFIX_PHONE: [
                "PHONE_NUMBER",
                "NAME_ID",
                "UPDATE_DATE",
                "PRIMARY_YN",
                "END_DATE",
                "PHONE_ROLE",
                "PHONE_TYPE",
            ],
        }

        self.join_keys = ["NAME_ID", "NAME_ID", "NAME_ID"]
        # self.transformations = {
        #     self.PREFIX_RESERVATION: lambda df: df,  # No transformation
        #     self.PREFIX_NAME: lambda df: self.common_lib.trim_upper(df),
        #     self.PREFIX_ADDRESS: lambda df: self.common_lib.trim_upper(df),
        #     self.PREFIX_PHONE: lambda df: self.common_lib.validate_email(
        #         self.common_lib.trim_lower(df),
        #         "EMAIL",
        #         128,
        #     )
        # }
        

        self.transformations = {
            self.PREFIX_RESERVATION: lambda df: df,  # No transformation
            self.PREFIX_NAME: lambda df: self.common_lib.trim_upper(df),
            self.PREFIX_ADDRESS: lambda df: self.common_lib.trim_upper(df),
            self.PREFIX_PHONE: lambda df: self.common_lib.validate_email(
                self.process_phone_email(
                    self.common_lib.trim_lower(df), "PHONE_NUMBER"
                ),
                "EMAIL",
                128,
            ),
        }

    # @staticmethod
    # def extract_date(filename):
    #     return regexp_extract(col("filename"), r"(\d{8})\.csv$", 1)

    def extract_file_prefix(self, file_path: str) -> str:
        """
        Extract file prefix handling both full and part files fro Loews
        Lowes file names have same substring uo_loews_name in 3 files
        Examples:
        - Full files: 20241112075930_uo_loews_name.csv
        - Part files: 20240912030006_uo_loews_name_001_of_002.csv
        """
        file_name = file_path.split("/")[-1].lower()

        # Remove timestamp prefix
        file_name = re.sub(r"^\d+_", "", file_name)

        # Remove part file pattern if present (e.g., _001_of_002.csv)
        file_name = re.sub(r"_\d{3}_of_\d{3}\.csv$", ".csv", file_name)

        # Remove .csv extension
        file_name = file_name.replace(".csv", "")

        logger.info(f"Processing cleaned filename: {file_name}")

        # Check for exact matches
        if file_name == "uo_loews_name_address":
            return self.PREFIX_ADDRESS
        elif file_name == "uo_loews_name_phone":
            return self.PREFIX_PHONE
        elif file_name == "uo_loews_name":
            return self.PREFIX_NAME
        elif file_name == "uo_loews_reservation_name":
            return self.PREFIX_RESERVATION

        logger.info(f"Warning: No matching prefix found for file {file_path}")
        return None

    def process_phone_email(self, df: DataFrame, phone_number_column: str) -> DataFrame:

        window_spec = Window.partitionBy("NAME_ID").orderBy(
            col("PRIMARY_YN").desc(), col("UPDATE_DATE").desc()
        )

        # Process email data, removed phone_type_code in the filter as per CR
        email_filter = (
            (lower(trim(col("PHONE_ROLE"))) == "email")
            & ((col("END_DATE").isNull()) | (trim(col("END_DATE")) == ""))
        )

        email_df = df.filter(email_filter)
        email_df = self.common_lib.validate_email(email_df, phone_number_column)
        email_df = email_df.withColumn("row_num", row_number().over(window_spec))
        email_df = email_df.filter(col("row_num") == 1).drop("row_num")
        email_df = email_df.withColumnRenamed(phone_number_column, "EMAIL")

        # Process phone data
        phone_filter = (lower(trim(col("PHONE_ROLE"))) == "phone") & (
            (col("END_DATE").isNull()) | (trim(col("END_DATE")) == "")
        )

        phone_df = df.filter(phone_filter)
        phone_df = phone_df.withColumn("row_num", row_number().over(window_spec))
        phone_df = phone_df.filter(col("row_num") == 1).drop("row_num")
        phone_df = phone_df.withColumnRenamed(phone_number_column, "PHONE")

        result_df = df.drop(
            phone_number_column, "PHONE_TYPE", "PHONE_ROLE", "PRIMARY_YN", "END_DATE"
        )  # Drop Non PII columns
        result_df = result_df.join(
            email_df.select("NAME_ID", "EMAIL"), "NAME_ID", "left"
        )
        result_df = result_df.join(
            phone_df.select("NAME_ID", "PHONE"), "NAME_ID", "left"
        )

        # logger.info("Final df after joining phone and email:")
        # result_df.show()
        return result_df

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
            logger.info(f"\nProcessing base_name: {base_name}")
            prefix = self.extract_file_prefix(base_name)
            logger.info(f"Extracted prefix: {prefix}")
            if prefix is None:
                error_message = f"Skipping files with no matching prefix: {base_name}"
                raise ValueError(error_message)

            if prefix not in self.schemas:
                error_message = f"No schema found for prefix: {prefix}"
                raise ValueError(error_message)

            schema = self.schemas[prefix]

            # Read and union all part files
            df = None
            for file in files:
                # temp_df = self.spark.read.options(
                #     delimiter="\t", header=False, quote="`"
                # ).csv(file)
                # Read file as RDD and split by tabs
                print("file reading into RDD is .....", file)
                rdd = self.spark.sparkContext.textFile(file)

                def process_line(line: str) -> list:
                    """Split line by tabs and ensure correct number of fields"""
                    fields = line.split("\t")
                    # Pad with empty strings if needed
                    while len(fields) < len(schema):
                        fields.append("")
                    # Trim if too many fields
                    return fields[: len(schema)]

                # Create DataFrame with schema column names directly
                processed_rdd = rdd.map(process_line)
                temp_df = processed_rdd.toDF(
                    schema
                )  # Use schema directly instead of temporary names

                # Check if the number of columns in the DataFrame matches the schema
                if len(temp_df.columns) != len(schema):
                    error_message = f"Number of columns in file ({len(temp_df.columns)}) does not match schema ({len(schema)}) for prefix: {prefix}"
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
                        logger.error(
                            f"ERROR: The following columns are not present in the DataFrame for prefix '{prefix}': {missing_columns}"
                        )
                        raise ValueError("Missing columns in the DataFrame")

                    if columns_to_keep:
                        df = df.select(*columns_to_keep)
                        logger.info(
                            f"Selected columns for prefix '{prefix}': {columns_to_keep}"
                        )
                    else:
                        logger.error(
                            f"ERROR: No valid columns to select for file prefix '{prefix}'."
                        )
                        raise ValueError("No valid columns to select")
                else:
                    logger.error(
                        f"ERROR: No columns specified for file prefix '{prefix}'."
                    )
                    raise ValueError("No columns specified")

                # Apply transformations after email and phone column seperation
                if prefix in self.transformations:
                    df = self.transformations[prefix](df)
                    logger.info(f"Applied transformations for prefix '{prefix}'")
                else:
                    logger.warning(
                        f"Warning: No transformations specified for prefix '{prefix}'"
                    )
                
                df = clean_all_date_fields(df, ['BIRTH_DATE'])

                # Handle duplicates, keeping the row with the maximum value in the dedupekey column
                if self.dedupekey in df.columns:
                    df_deduped = self.common_lib.deduplicate_by_timestamp(
                    df=df,
                    dedupekey=self.dedupekey,
                    timestamp_format="MM/dd/yyyy hh:mm:ss a",
                    validate_timestamps=True,
                    drop_invalid_timestamps=False,
                    preserve_case=True
                    )
                    df=df_deduped
                    
                
                    if prefix in [self.base_df_name, self.PREFIX_ADDRESS,self.PREFIX_PHONE]:
                        pass  # keep the dedupekey
                    else:
                        df = df.drop(self.dedupekey)
                    
                else:
                    logger.warning(
                        f"Warning: {self.dedupekey} column not found in DataFrame for prefix '{prefix}'. Skipping deduplication."
                    )

                logger.info(
                    f"Sending records for joins of file group with prefix: {prefix}"
                )
                print(df.count())

                # Add the processed DataFrame to the dictionary
                df_dict[prefix] = df

        logger.info("\nFinal df_dict contents:")
        for key, value in df_dict.items():
            logger.info(f"Key: {key}")
            logger.info(f"Columns: {value.columns}")
            logger.info(f"Row count: {value.count()}")

        # Define join mapping
        join_mapping = {
            "uo_loews_reservation_name": "NAME_ID",
            "uo_loews_name": "NAME_ID",
            "uo_loews_name_address": "NAME_ID",
            "uo_loews_name_phone": "NAME_ID",
        }

        # Perform the join
        # final_df = self.join_dataframes(df_dict, join_mapping)
        final_df = self.join_dataframes(
            df_dict, join_mapping, self.base_df_name
        )

        logger.info("record count after join:")
        logger.info(final_df.count())
        

        return final_df
    
    def join_dataframes(self, df_dict, join_mapping, base_df_name):
        # Start with LOEWS_RES_NAME (LRN)
        base_df = df_dict[base_df_name]
        initial_count = base_df.count()
        logger.info(f"Initial base table count: {initial_count}")
        
        base_df = base_df.alias('base')
    
        # Join with LOEWS_NAME (LN)
        ln_df = df_dict['uo_loews_name'].alias('LN')
        
        base_df = base_df.join(
            ln_df,
            base_df['base.NAME_ID'] == ln_df['LN.NAME_ID'],
            'left'
        )
    
        # Join with LOEWS_NAME_ADDRESS (LNA)
        lna_df = df_dict['uo_loews_name_address'].alias('LNA')
        window_spec = Window.partitionBy('NAME_ID').orderBy(
            col('PRIMARY_YN').desc(),
            col('UPDATE_DATE').desc()
        )
        
        lna_df = lna_df.withColumn(
            'LNA_LATEST',
            row_number().over(window_spec)
        ).filter(col('LNA_LATEST') == 1)
    
        base_df = base_df.join(
            lna_df,
            ln_df['LN.NAME_ID'] == lna_df['LNA.NAME_ID'],
            'left'
        )
    
        # Join with PHONE for email and phone
        lnp_df = df_dict['uo_loews_name_phone'].alias('LNP')
        lnp_window = Window.partitionBy('NAME_ID').orderBy(
            col('UPDATE_DATE').desc()
        )
        
        lnp_df = lnp_df.withColumn(
            'LNP_LATEST',
            row_number().over(lnp_window)
        ).filter(col('LNP_LATEST') == 1)
    
        base_df = base_df.join(
            lnp_df,
            ln_df['LN.NAME_ID'] == lnp_df['LNP.NAME_ID'],
            'left'
        )
    
        # Debug: Print all available columns
        logger.info("Available columns in base_df:")
        for column in base_df.columns:
            logger.info(column)
    
        # Select columns with proper handling of table prefixes
        columns_to_select = [
            col('base.NAME_ID').alias('NAME_ID'),
            col('base.UPDATE_DATE').alias('UPDATE_DATE'),
            col('base.INSERT_DATE').alias('INSERT_DATE'),
            col('base.RESV_NAME_ID').alias('RESV_NAME_ID'),
            col('LN.BIRTH_DATE').alias('BIRTH_DATE'),
            col('LN.MIDDLE').alias('MIDDLE'),
            col('LN.TITLE').alias('TITLE'),
            col('LN.FIRST').alias('FIRST'),
            col('LN.LAST').alias('LAST'),
            col('LNA.STATE').alias('STATE'),
            col('LNA.CITY').alias('CITY'),
            col('LNA.COUNTRY').alias('COUNTRY'),
            col('LNA.ZIP_CODE').alias('ZIP_CODE'),
            col('LNA.ADDRESS1').alias('ADDRESS1'),
            col('LNA.ADDRESS2').alias('ADDRESS2'),
            col('LNA.PRIMARY_YN').alias('PRIMARY_YN'),
            col('LNP.EMAIL').alias('EMAIL'),
            col('LNP.PHONE').alias('PHONE')
        ]
    
        # Create final dataframe with selected columns
        final_df = base_df.select(*columns_to_select)
        
        # Debug: Print final columns
        # logger.info("Final columns in final_df after join:")
        # final_df.show(truncate=False)
        
        # Final count check
        final_count = final_df.count()
        logger.info(f"\nFinal record count: {final_count}")
        if final_count != initial_count:
            logger.warning(
                f"Final count ({final_count}) differs from initial count ({initial_count})"
            )
    
        return final_df
    

    def map_to_common_schema(self, source_df: DataFrame, source) -> DataFrame:
        # Define the mapping from source columns to target columns

        column_mapping = {
            "source_name":lit(source),  # Using the source parameter
            "source_customer_id": col("RESV_NAME_ID"),
            "prefix": col("TITLE"),
            "first_name": col("FIRST"),
            "middle_name": col("MIDDLE"),
            "last_name": col("LAST"),
            "generation_qualifier": lit(""),  # Not present in source, set to NULL
            "gender": lit(""),  # Not present in source, set to NULL
            "birth_date": col("BIRTH_DATE"),
            "address_line_1": col("ADDRESS1"),
            "address_line_2": col("ADDRESS2"),
            "city": col("CITY"),
            "state_code": col("STATE"),
            "postal_code": col("ZIP_CODE"),
            "country_code": col("COUNTRY"),
            "phone": col("PHONE"),  # Using the new transformed PHONE column
            "email_address": col("EMAIL"),  # Using the new EMAIL column
            "guest_id": lit(""),  # col("RESV_NAME_ID")
            "record_created_timestamp": col("INSERT_DATE"),
            "record_last_updated_timestamp": col("UPDATE_DATE"),
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
