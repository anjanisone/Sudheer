# ---------------------------------------------------------------------------
# dataframe_handler.py  — complete with embedded date/timestamp cleanup
# ---------------------------------------------------------------------------

import pandas as pd
from datetime import datetime, timezone

# ---------- Embedded Date/Timestamp Cleanup (Spark) ----------
def clean_all_date_fields(df):
    """
    Cleans and standardizes all date/timestamp fields in a Spark DataFrame.

    - Trims whitespace
    - Replaces placeholders with null
    - Parses multiple date/time formats (including month names)
    - Nullifies out-of-range or future values
    - Formats all valid dates/timestamps into ISO 8601

    Returns: Spark DataFrame
    """
    from datetime import datetime
    from pyspark.sql import functions as F

    now = datetime.now()
    current_year = now.year
    min_year = 1900

    invalid_values = ["null", "none", "n/a", "missing", "not null", "", "na"]
    date_cols = [c for c in df.columns if any(x in c.lower() for x in ["date", "timestamp", "time"])]

    for c in date_cols:
        # Trim and replace invalid placeholders
        df = df.withColumn(c, F.trim(F.col(c)))
        df = df.withColumn(c, F.when(F.col(c).isin(invalid_values), None).otherwise(F.col(c)))

        # Try multiple parsing formats (date & timestamp)
        parsed = F.coalesce(
            F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.col(c), "yyyy/MM/dd HH:mm:ss"),
            F.to_timestamp(F.col(c), "yyyyMMddHHmmss"),
            F.to_timestamp(F.col(c), "yyyy-MM-dd'T'HH:mm:ss"),
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

        # Nullify invalid or future values
        df = df.withColumn(
            c,
            F.when(
                (F.year(F.col(c)) < min_year) |
                (F.year(F.col(c)) > current_year) |
                (F.col(c) > F.lit(now)),
                None
            ).otherwise(F.col(c))
        )

        # Convert to ISO 8601 format
        iso_format = (
            "yyyy-MM-dd'T'HH:mm:ss'Z'"
            if ("timestamp" in c.lower() or "time" in c.lower())
            else "yyyy-MM-dd"
        )
        df = df.withColumn(c, F.date_format(F.col(c), iso_format))

    return df


class DataFrameHandler:
    @staticmethod
    def get_col_prefix(col_name: str) -> str:
        try:
            renamed_cols = {
                "source_name": "source_name",
                "source_customer_id": "source_customer_id",
                "record_created_timestamp": "record_created_timestamp",
                "record_last_updated_timestamp": "record_last_updated_timestamp",
                "guest_id": "rules_guest_id",
            }
            if col_name in renamed_cols:
                return renamed_cols[col_name]
            if col_name == "pass_thru_data":
                return col_name
            return f"ml_{col_name}"
        except Exception as e:
            print(f"Error: Failed at get_col_prefix() with error {e}")
            raise e

    @staticmethod
    def map_input_output_cols(df: pd.DataFrame, input_cols, output_cols) -> pd.DataFrame:
        try:
            col_mapping = dict(zip(input_cols, output_cols[: len(input_cols)]))
            new_df = pd.DataFrame()

            # Map input -> standardized names
            for old_col, new_col in col_mapping.items():
                new_col_name = DataFrameHandler.get_col_prefix(new_col)
                new_df[new_col_name] = df[old_col] if old_col in df.columns else None

            # Ensure all output columns exist
            for col in output_cols:
                if col not in col_mapping.values():
                    new_col_name = DataFrameHandler.get_col_prefix(col)
                    new_df[new_col_name] = df[col] if col in df.columns else None

            return new_df
        except Exception as e:
            print(f"Error: Failed at map_input_output_cols() with error {e}")
            raise e

    @staticmethod
    def create_tmst_cols(new_df: pd.DataFrame) -> pd.DataFrame:
        try:
            # 1) Clean and standardize all date/timestamp fields first
            new_df = clean_all_date_fields(new_df)

            # 2) Generate tx_datetime (UTC, millisecond precision)
            current_datetime = datetime.now(timezone.utc)
            new_df["current_tmst"] = pd.to_datetime(current_datetime, utc=True)
            new_df["tx_datetime"] = new_df["current_tmst"].dt.floor("ms")
            new_df["tx_datetime"] = new_df["tx_datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str[:-3] + "Z"
            new_df["tx_datetime"] = pd.to_datetime(new_df["tx_datetime"], format="%Y-%m-%dT%H:%M:%S.%fZ")

            # 3) Preserve created vs updated
            new_df = new_df.drop("current_tmst", axis=1)
            new_df["record_last_updated_timestamp"] = new_df.apply(
                lambda row: row["record_created_timestamp"]
                if row.get("record_last_updated_timestamp") in ("", None)
                else row.get("record_last_updated_timestamp"),
                axis=1,
            )

            # 4) Replace remaining NaNs with empty strings (if your downstream expects strings)
            new_df = new_df.apply(lambda x: x.fillna(""))

            return new_df
        except Exception as e:
            print(f"Error: Failed at create_tmst_cols() with error {e}")
            raise e
