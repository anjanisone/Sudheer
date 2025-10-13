# ---------------------------------------------------------------------------
# dataframe_handler.py  — complete with embedded date/timestamp cleanup
# ---------------------------------------------------------------------------

import pandas as pd
from datetime import datetime, timezone

# ---------- Embedded Date/Timestamp Cleanup (Pandas) ----------
def clean_all_date_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and standardizes all date/timestamp fields (Pandas):
      - Trim & normalize
      - Replace placeholders with null
      - Parse many date/time formats (incl. month names)
      - Nullify out-of-range (year<1900 or >current) and future values
      - Output formats:
          * Date-like    -> YYYY-MM-DD
          * Timestamp-like-> YYYY-MM-DDTHH:MM:SS.mmmZ
    """
    now = datetime.now()
    current_year = now.year
    min_year = 1900
    invalid_values = {"null", "none", "n/a", "missing", "not null", "", "na"}

    # Work on a copy to avoid mutating caller unintentionally
    df = df.copy()

    for col in list(df.columns):
        if any(x in col.lower() for x in ["date", "timestamp", "time"]):
            # Normalize text / placeholders
            df[col] = df[col].apply(lambda v: None if (v is None) else str(v).strip())
            df[col] = df[col].apply(lambda v: None if (v is None or str(v).lower() in invalid_values) else v)

            # Try a range of formats (vectorized-ish passes with errors='ignore')
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y/%m/%d %H:%M:%S",
                "%Y%m%d%H%M%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%m/%d/%Y",
                "%d-%m-%Y",
                "%d/%m/%Y",
                "%Y%m%d",
                "%b %d, %Y",   # Aug 06, 2023
                "%B %d, %Y",   # August 06, 2023
                "%d-%b-%Y",    # 06-Aug-2023
                "%d-%B-%Y",    # 06-August-2023
            ]
            # Progressive parsing: only convert values still strings or not yet datetimes
            for fmt in formats:
                try:
                    mask = df[col].notna()
                    df.loc[mask, col] = pd.to_datetime(df.loc[mask, col], format=fmt, errors="ignore")
                except Exception:
                    # keep going on format failures
                    pass

            # Nullify out-of-range or future
            def _nullify(v):
                if pd.isna(v):
                    return None
                if not isinstance(v, pd.Timestamp):
                    return v  # already string that failed parsing; keep as-is and will be turned None below
                if v.year < min_year or v.year > current_year or v.to_pydatetime() > now:
                    return None
                return v

            df[col] = df[col].apply(_nullify)

            # Final formatting to ISO
            if "timestamp" in col.lower() or "time" in col.lower():
                df[col] = df[col].apply(
                    lambda x: x.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" if isinstance(x, pd.Timestamp) else None
                )
            else:
                df[col] = df[col].apply(
                    lambda x: x.strftime("%Y-%m-%d") if isinstance(x, pd.Timestamp) else None
                )

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
