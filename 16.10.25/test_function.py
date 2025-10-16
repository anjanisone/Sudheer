from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from clean_module import clean_all_date_fields  # adjust import path if needed

spark = (
    SparkSession.builder
    .appName("CleanAllDateFieldsTest")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
)

data = [
    ("2023-08-06", "Aug 06, 2023", "2025-12-31 12:00:00", "2023/05/06", "none"),
    ("2023/01/01", "January 01, 2023", "1899-12-31", "2023-08-06T12:30:00", "missing"),
    ("06-08-2023", "06-Aug-2023", "2050-01-01", "2023-07-01", ""),
    ("20230806", "August 06, 2023", "2023/06/30", "2023-02-28", "na"),
    ("invalid", None, "2024/05/01", "Aug 07, 2023", "2023-09-01")
]

columns = ["col1", "col2", "col3", "col4", "col5"]

before_df = spark.createDataFrame(data, columns)

print("=== BEFORE CLEANUP ===")
before_df.show(truncate=False)

after_df = clean_all_date_fields(before_df, ["col1", "col2", "col3", "col4", "col5"])

print("=== AFTER CLEANUP ===")
after_df.show(truncate=False)
