import sys
import os
import pandas as pd

project_root = os.path.abspath("universal-customer-mesh/src")
sys.path.insert(0, project_root)

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

data = {
    "birth_date": ["Aug 06, 2023", "2050-01-01", "1899-12-31", "2023/02/28", " 2023-05-07 ", "n/a"],
    "record_created_timestamp": [
        "2023-05-06 13:45:00",
        "2025-11-01 10:30:00",
        "2020/07/15 09:00:00",
        "Aug 07, 2023",
        "invalid",
        None,
    ],
    "record_last_updated_timestamp": [
        "06-Aug-2023",
        "2023-08-06T15:30:00",
        "2026-12-31 23:59:59",
        "August 06, 2023",
        "2022-02-28",
        "",
    ],
    "opt_in_date": ["2023/06/01", "none", "2023-02-28", "Feb 30, 2023", "2023-07-01", "2023-08-06"],
}

before_df = pd.DataFrame(data)
after_df = clean_all_date_fields(before_df)

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 160)

print("=== BEFORE CLEANUP ===")
print(before_df)

print("\n=== AFTER CLEANUP ===")
print(after_df)
