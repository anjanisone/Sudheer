import sys
import os
import pandas as pd
from pyspark.sql import SparkSession

project_root = os.path.abspath("universal-customer-mesh/src")
sys.path.insert(0, project_root)

from glue_jobs.large_source_etl_job.large_source_etl_job import clean_all_date_fields

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

spark = SparkSession.builder.master("local[*]").appName("DateCleanupTest").getOrCreate()
before_df = spark.createDataFrame(pd.DataFrame(data))

after_df = clean_all_date_fields(before_df)

print("=== BEFORE CLEANUP ===")
before_df.show(truncate=False)

print("\n=== AFTER CLEANUP ===")
after_df.show(truncate=False)
