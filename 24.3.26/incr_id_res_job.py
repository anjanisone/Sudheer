import sys
import json
import logging
import boto3
import pytz
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from batch_data import get_next_candidate_identities_batch
from batch_data import get_next_candidate_identities_update_status_dydb, check_incr_control_job_status
from args_and_constants import get_job_args_incremental
from historical_data import convert_spark_dataframe_columns_to_string
from id_resolution import perform_id_resolution_single_batch
from id_res_utils import remove_all_cached_spark_dataframes
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, from_json, lit, lower, trim, when
from pyspark.sql.types import StringType, StructField, StructType
import time


# --- Reverse-append preflight (inline in this job script; no separate helper module) ---

EMAIL_COL = "ml_email_address"
FLAG_COL = "reverse_append_enriched"
EMAIL_KEY_COL = "__reverse_append_email_key"
ENRICH_JSON_ATTR = "enriched_json"
ENRICHABLE_ML_COLUMNS = [
    "ml_first_name",
    "ml_last_name",
    "ml_address_line_1",
    "ml_address_line_2",
    "ml_city",
    "ml_state_code",
    "ml_postal_code",
    "ml_country_code",
]


def _preflight_enabled(raw: str) -> bool:
    return raw is not None and str(raw).strip().lower() in ("true", "1", "yes")


def _ddb_batch_get_email_payloads(table_name: str, email_keys: List[str]) -> Dict[str, Dict[str, Any]]:
    if not email_keys or not table_name or table_name.strip().lower() in ("", "disabled", "none"):
        return {}
    client = boto3.client("dynamodb")
    found: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(email_keys), 100):
        chunk = email_keys[i : i + 100]
        request_items = {
            table_name: {"Keys": [{"email_key": {"S": k}} for k in chunk], "ConsistentRead": False}
        }
        while request_items:
            resp = client.batch_get_item(RequestItems=request_items)
            for item in resp.get("Responses", {}).get(table_name, []):
                ek = item.get("email_key", {}).get("S")
                if not ek:
                    continue
                payload = item.get(ENRICH_JSON_ATTR, {}).get("S")
                if payload:
                    try:
                        found[ek] = json.loads(payload)
                    except json.JSONDecodeError:
                        logging.getLogger(__name__).warning(
                            "Invalid JSON in %s for email_key=%s", ENRICH_JSON_ATTR, ek
                        )
            request_items = resp.get("UnprocessedKeys") or {}
    return found


def _ddb_put_email_payloads(table_name: str, email_to_payload: Dict[str, Dict[str, Any]]) -> None:
    if not email_to_payload:
        return
    table = boto3.resource("dynamodb").Table(table_name)
    with table.batch_writer(overwrite_by_pkeys=["email_key"]) as batch:
        for email_key, payload in email_to_payload.items():
            batch.put_item(Item={"email_key": email_key, ENRICH_JSON_ATTR: json.dumps(payload)})


def _secrets_get_json(secret_arn: str) -> Dict[str, str]:
    if not secret_arn or secret_arn.strip().upper() in ("", "DISABLED", "NONE", "NOT_CONFIGURED"):
        return {}
    resp = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)
    return json.loads(resp.get("SecretString") or "{}")


def _snowflake_jdbc_url_and_props(secret: Dict[str, str]) -> Tuple[str, Dict[str, str]]:
    account = secret.get("account") or secret.get("ACCOUNT")
    user = secret.get("user") or secret.get("username") or secret.get("USER")
    password = secret.get("password") or secret.get("PASSWORD")
    warehouse = secret.get("warehouse") or secret.get("WAREHOUSE")
    database = secret.get("database") or secret.get("DATABASE")
    schema = secret.get("schema") or secret.get("SCHEMA", "PUBLIC")
    if not all([account, user, password]):
        raise ValueError("Snowflake secret must include account, user, and password")
    host = account if ".snowflakecomputing.com" in str(account) else f"{account}.snowflakecomputing.com"
    url = f"jdbc:snowflake://{host}/"
    params = []
    if warehouse:
        params.append(f"warehouse={warehouse}")
    if database:
        params.append(f"db={database}")
    if schema:
        params.append(f"schema={schema}")
    if params:
        url = url + "?" + "&".join(params)
    props = {
        "user": user,
        "password": password,
        "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
    }
    return url, props


def _snowflake_fetch_by_emails(
    spark: SparkSession,
    secret_arn: str,
    table_fqn: str,
    email_column: str,
    missing_emails: List[str],
) -> Dict[str, Dict[str, Any]]:
    if not missing_emails:
        return {}
    secret = _secrets_get_json(secret_arn)
    jdbc_url, props = _snowflake_jdbc_url_and_props(secret)
    result: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(missing_emails), 200):
        chunk = missing_emails[i : i + 200]
        quoted = ",".join("'" + e.replace("'", "''") + "'" for e in chunk)
        subquery = (
            f"(SELECT * FROM {table_fqn} WHERE LOWER(TRIM({email_column})) IN ({quoted})) AS reverse_append_sub"
        )
        sdf = spark.read.jdbc(url=jdbc_url, table=subquery, properties=props)
        if sdf.rdd.isEmpty():
            continue
        for row in sdf.collect():
            rd_lower = {k.lower(): v for k, v in row.asDict().items()}
            raw_email = rd_lower.get(email_column.lower())
            if raw_email is None:
                continue
            ek = str(raw_email).strip().lower()
            payload = {}
            for ml in ENRICHABLE_ML_COLUMNS:
                v = rd_lower.get(ml.lower())
                if v is not None:
                    payload[ml] = str(v)
            if payload:
                result[ek] = payload
    return result


def _enrichment_struct_schema():
    return StructType([StructField(c, StringType(), True) for c in ENRICHABLE_ML_COLUMNS])


def run_reverse_append_preflight(df_batch, spark: SparkSession, job_args: Dict[str, Any]):
    """
    Normalize email → DynamoDB cache → Snowflake on miss → write cache → merge ml_* fields.
    No-op when reverse_append_preflight_enabled is false.
    """
    if not _preflight_enabled(job_args.get("reverse_append_preflight_enabled", "false")):
        return df_batch

    cache_table = (job_args.get("reverse_append_cache_table") or "").strip()
    secret_arn = (job_args.get("snowflake_jdbc_secret_arn") or "").strip()
    table_fqn = (job_args.get("snowflake_enrichment_table_fqn") or "").strip()
    email_column = (job_args.get("snowflake_email_column") or "EMAIL").strip()

    if EMAIL_COL not in df_batch.columns:
        logger.warning("Column %s missing; skipping reverse-append preflight.", EMAIL_COL)
        return df_batch

    df_batch = df_batch.withColumn(
        EMAIL_KEY_COL,
        when(
            col(EMAIL_COL).isNull() | (trim(col(EMAIL_COL)) == lit("")),
            lit(None).cast(StringType()),
        ).otherwise(lower(trim(col(EMAIL_COL)))),
    )

    if not cache_table or cache_table.lower() in ("disabled", "none"):
        logger.warning("Preflight enabled but reverse_append_cache_table missing; skipping.")
        return df_batch.drop(EMAIL_KEY_COL) if EMAIL_KEY_COL in df_batch.columns else df_batch

    keys_df = df_batch.where(col(EMAIL_KEY_COL).isNotNull()).select(EMAIL_KEY_COL).distinct()
    email_keys = [r[0] for r in keys_df.collect()]
    if not email_keys:
        out = df_batch.withColumn(FLAG_COL, lit("N"))
        return out.drop(EMAIL_KEY_COL)

    from_cache = _ddb_batch_get_email_payloads(cache_table, email_keys)
    missing = [k for k in email_keys if k not in from_cache]

    from_sf: Dict[str, Dict[str, Any]] = {}
    if missing and secret_arn.upper() not in ("DISABLED", "NONE", "") and table_fqn.upper() not in (
        "NOT_CONFIGURED",
        "",
        "NONE",
    ):
        try:
            from_sf = _snowflake_fetch_by_emails(spark, secret_arn, table_fqn, email_column, missing)
            _ddb_put_email_payloads(cache_table, from_sf)
        except Exception as ex:
            logger.error("Snowflake preflight failed: %s", ex)
            raise
    elif missing:
        logger.warning(
            "%d emails not in DynamoDB cache; Snowflake not configured — proceeding without enrichment.",
            len(missing),
        )

    combined = {**from_cache, **from_sf}
    if not combined:
        out = df_batch.withColumn(FLAG_COL, lit("N"))
        return out.drop(EMAIL_KEY_COL)

    rows = [(k, json.dumps(v)) for k, v in combined.items()]
    enrich_df = spark.createDataFrame(rows, [EMAIL_KEY_COL, "__enrich_json"])
    merged = df_batch.join(enrich_df, EMAIL_KEY_COL, "left")
    merged = merged.withColumn("__enrich_struct", from_json(col("__enrich_json"), _enrichment_struct_schema()))

    out = merged
    for c in ENRICHABLE_ML_COLUMNS:
        enrich_col = col(f"__enrich_struct.{c}")
        if c in out.columns:
            out = out.withColumn(c, coalesce(col(c), enrich_col))
        else:
            out = out.withColumn(c, enrich_col)

    out = out.withColumn(
        FLAG_COL,
        when(col("__enrich_json").isNotNull(), lit("Y")).otherwise(lit("N")),
    )
    return out.drop(EMAIL_KEY_COL, "__enrich_json", "__enrich_struct")


# Spark contexts and configs
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
job = Job(glueContext)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logger = logging.getLogger(__name__)


def should_shutdown_job(start_time):
    """
    Check if the job should be shutdown based on duration and time of day
    """
    current_time = datetime.now(pytz.UTC)
    job_duration = current_time - start_time

    # Check if job has run for more than 22 hours
    duration_check = job_duration > timedelta(hours=22)
    return duration_check


def main():
    try:

        job_start_time = datetime.now(pytz.UTC)
        job_args = get_job_args_incremental()
        dydb_table_name = job_args["dydb_table"]

        max_items = job_args["max_items"]
        max_records = job_args["max_records"]
        sources = job_args["incremental_data_sources_enabled"].strip()
        incremental_common_schema_data_override: str = job_args["incremental_common_schema_data_override"]
        expire_in_days = job_args["expire_in_days"]
        ctrl_incr_key = job_args["ctrl_incr_key"]
        incr_ctrl_table = job_args["incr_ctrl_table"]

        expire_in_days = int(expire_in_days)

        # set spark checkpoint dir, which is required by the graphframes library
        tmp_spark_checkpoint_s3_path: str = f"s3://{job_args['temp_s3_bucket']}/tmp_spark_checkpoint/"
        print("Set temp spark checkpoint folder to:", tmp_spark_checkpoint_s3_path)
        spark.sparkContext.setCheckpointDir(tmp_spark_checkpoint_s3_path)

        while True:

            if should_shutdown_job(job_start_time):
                logger.info("Shutting down the job as it has been running for more than 22 hours.")
                break

            if incremental_common_schema_data_override.lower().strip() != "not_overridden_use_default":
                print(
                    f"incremental_common_schema_data_override={incremental_common_schema_data_override} is set, so reading override common schema data for id resolution"
                )
                df_batch = spark.read.parquet(incremental_common_schema_data_override)
                batch_files = [incremental_common_schema_data_override]
            else:
                # Check for batches of Common Schema data to load for the id resolution process
                df_batch, batch_files = get_next_candidate_identities_batch(
                    dydb_table_name,
                    sources,
                    max_items,
                    max_records,
                    spark_context=spark,
                    glue_context=glueContext,
                )

            # exit in case of no records
            logger.info("List of Batch files: %s", batch_files)
            if not df_batch:
                logger.info("No more data batches to process. Shutting down this job.")
                break

            df_batch = convert_spark_dataframe_columns_to_string(df_batch)
            df_batch = run_reverse_append_preflight(df_batch, spark, job_args)

            perform_id_resolution_single_batch(
                df_batch,
                spark_context=spark,
            )

            if incremental_common_schema_data_override.lower().strip() != "not_overridden_use_default":
                print("Done with incremental job given incremental_common_schema_data_override override setting. Exiting.")
                break
            else:
                ttl_value = int(time.time()) + (int(expire_in_days) * 24 * 60 * 60)
                get_next_candidate_identities_update_status_dydb(
                    dydb_table_name, batch_files, expire_in_days, "main-job-in-progress", "main-job-complete", ttl_value
                )
            remove_all_cached_spark_dataframes(spark, sc)
            ctrl_job_status = check_incr_control_job_status(incr_ctrl_table, ctrl_incr_key)
            if ctrl_job_status.strip().lower() == "true":
                logger.info("ctrl_job_status is set to TRUE,  Exiting the Job")
                break

    except Exception as e:
        logger.error("Error in job execution & updating failed-status to dynamo")
        logger.error("Error in job execution: %s", str(e))
        if incremental_common_schema_data_override.lower().strip() == "not_overridden_use_default":
            get_next_candidate_identities_update_status_dydb(
                dydb_table_name, batch_files, expire_in_days, "main-job-in-progress", "main-job-failed"
            )
        else:
            print(
                "No need to update DDB to main-job-failed even though failure occured, because incremental_common_schema_data_override is set."
            )
        raise
    finally:
        job_duration = datetime.now(pytz.UTC) - job_start_time
        logger.info("Total job duration: %s", str(job_duration))


if __name__ == "__main__":
    main()
