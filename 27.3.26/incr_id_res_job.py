import base64
import hashlib
import sys
import json
import logging
import boto3
from urllib.parse import quote_plus
from boto3.dynamodb.types import TypeDeserializer
import pytz
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

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
from rules.pre_search_rules.Rule_0600_EmailCleaner import Rule_0600_EmailCleaner
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, current_timestamp, from_json, lit, lower, sha2, trim, upper, when
from pyspark.sql.types import BooleanType, StringType, StructField, StructType
import time


# --- Reverse-append preflight (inline in this job script; no separate helper module) ---

EMAIL_COL = "ml_email_address"
STATUS_COL = "reverse_append_lookup_status"
EMAIL_KEY_COL = "__reverse_append_email_key"
EMAIL_HASH_COL = "__reverse_append_email_hash"
FOUND_IN_DDB = "Found in DDB"
NOTFOUND_BUT_ADDED = "NotFound But Added"
NOTFOUND_NOT_ADDED = "NotFound & Not Added"
DEFAULT_REVERSE_INPUT_TABLE = "STAGE.NBCU_REVERSE_INPUT"
DEFAULT_REVERSE_CTRL_TABLE = "STAGE.NBCU_REVERSE_CTRL"
DEFAULT_REVERSE_OUTPUT_TABLE = "STAGE.NBCU_REVERSE_OUTPUT"

# Run in Snowflake (not from Glue). Customizable via DATABASE.SCHEMA prefixes for your account.
NBCU_REVERSE_APPEND_TASK_DDL = r"""
CREATE OR REPLACE TASK CUSTOMER_360.STAGE.NBCU_REVERSE_TASK
  WAREHOUSE = DI_XSM_T5_03_WH
  SCHEDULE = '1 MINUTE'
AS
  CALL CUSTOMER_360.STAGE.EXECUTE_REVERSE_APPEND(
    (SELECT BATCH_ID
       FROM CUSTOMER_360.STAGE.NBCU_REVERSE_CTRL
      WHERE STATUS = 'NEW'
      ORDER BY CREATED_TS
      LIMIT 1)
  );

ALTER TASK CUSTOMER_360.STAGE.NBCU_REVERSE_TASK RESUME;
""".strip()

# Override with Glue arg --rea_reverse_append_debug_s3 for other environments.
REA_MERGED_DEBUG_S3_DEFAULT = "s3://udx-cust-mesh-d-incr-data-out/intermediate_data/debugging/rea_debugging/"

ENRICHABLE_ML_COLUMNS = [
    "ml_first_name",
    "ml_last_name",
    "ml_address_line_1",
    "ml_address_line_2",
    "ml_city",
    "ml_state_code",
    "ml_postal_code",
    "ml_country_code",
    "ml_phone",
]


def _preflight_enabled(raw: str) -> bool:
    return raw is not None and str(raw).strip().lower() in ("true", "1", "yes")


def _optional_arg(name: str, default: str = "") -> str:
    flag = f"--{name}"
    for i, a in enumerate(sys.argv):
        if a == flag and i + 1 < len(sys.argv):
            return str(sys.argv[i + 1])
    return default


def _rea_merged_debug_s3_path() -> str:
    """Parquet snapshot after reverse-append preflight (merged batch entering ID resolution)."""
    explicit = _optional_arg("rea_reverse_append_debug_s3", "").strip()
    if explicit:
        return explicit.rstrip("/") + "/"
    return REA_MERGED_DEBUG_S3_DEFAULT


def _write_rea_merged_debug_parquet(df_batch, path: str) -> None:
    p = path.rstrip("/") + "/"
    logging.getLogger(__name__).info("Writing reverse-append merged batch to %s", p)
    df_batch.write.mode("overwrite").parquet(p)


_ddb_deser = TypeDeserializer()


def _ddb_av_to_plain(item: Dict[str, Any]) -> Dict[str, Any]:
    if not item:
        return {}
    return {k: _ddb_deser.deserialize(v) for k, v in item.items()}


def _ddb_batch_get_by_email_hash(table_name: str, email_hashes: List[str]) -> Dict[str, Dict[str, Any]]:
    """Batch-get reverse-append rows keyed by partition key email_hash (uppercase hex SHA-256)."""
    if not email_hashes or not table_name or table_name.strip().lower() in ("", "disabled", "none"):
        return {}
    client = boto3.client("dynamodb")
    found: Dict[str, Dict[str, Any]] = {}
    normalized = list({h.upper() for h in email_hashes if h})
    for i in range(0, len(normalized), 100):
        chunk = normalized[i : i + 100]
        request_items = {
            table_name: {"Keys": [{"email_hash": {"S": h}} for h in chunk], "ConsistentRead": False}
        }
        while request_items:
            resp = client.batch_get_item(RequestItems=request_items)
            for item in resp.get("Responses", {}).get(table_name, []):
                plain = _ddb_av_to_plain(item)
                eh = plain.get("email_hash")
                if eh is not None:
                    found[str(eh).upper()] = plain
            request_items = resp.get("UnprocessedKeys") or {}
    return found


def _ddb_flat_row_to_ml_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    """Map DynamoDB export-style column names to ml_* enrichment keys."""

    def pick(*names: str) -> Optional[str]:
        for n in names:
            if n not in row or row[n] is None:
                continue
            s = str(row[n]).strip()
            if not s or s.lower() == "null":
                continue
            return s
        return None

    out: Dict[str, Any] = {}
    v = pick("first_name")
    if v:
        out["ml_first_name"] = v
    v = pick("last_name")
    if v:
        out["ml_last_name"] = v
    v = pick("address_line_1")
    if v:
        out["ml_address_line_1"] = v
    v = pick("address_line_2")
    if v:
        out["ml_address_line_2"] = v
    v = pick("city")
    if v:
        out["ml_city"] = v
    v = pick("state", "state_code")
    if v:
        out["ml_state_code"] = v
    v = pick("postal_code", "zip", "zipcode")
    if v:
        out["ml_postal_code"] = v
    v = pick("country", "country_code")
    if v:
        out["ml_country_code"] = v
    v = pick("phone_number", "phone", "ml_phone")
    if v:
        out["ml_phone"] = v
    return out


def _ml_payload_to_reverse_append_item(
    email_norm: str,
    email_hash_u: str,
    ml: Dict[str, Any],
    batch_id: str,
    source_ds: str,
) -> Dict[str, Any]:
    """Build a flat DynamoDB item consistent with udx reverse-append table / results CSV."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    rev = {
        "first_name": ml.get("ml_first_name"),
        "last_name": ml.get("ml_last_name"),
        "address_line_1": ml.get("ml_address_line_1"),
        "address_line_2": ml.get("ml_address_line_2"),
        "city": ml.get("ml_city"),
        "state": ml.get("ml_state_code"),
        "postal_code": ml.get("ml_postal_code"),
        "country": ml.get("ml_country_code"),
        "phone_number": ml.get("ml_phone"),
    }
    item: Dict[str, Any] = {
        "email_hash": email_hash_u.upper(),
        "email_address": email_norm,
        "batch_id": batch_id,
        "load_timestamp": ts,
        "source_ds": source_ds,
    }
    for k, v in rev.items():
        if v is not None and str(v).strip() != "":
            item[k] = str(v).strip()
    return item


def _ddb_put_reverse_append_items(table_name: str, items: List[Dict[str, Any]]) -> None:
    if not items or not table_name or table_name.strip().lower() in ("", "disabled", "none"):
        return
    table = boto3.resource("dynamodb").Table(table_name)
    with table.batch_writer(overwrite_by_pkeys=["email_hash"]) as batch:
        for it in items:
            batch.put_item(Item={k: v for k, v in it.items() if v is not None and v != ""})


def _secrets_get_json(secret_arn: str) -> Dict[str, Any]:
    if not secret_arn or secret_arn.strip().upper() in ("", "DISABLED", "NONE", "NOT_CONFIGURED"):
        return {}
    resp = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)
    return json.loads(resp.get("SecretString") or "{}")


def _sf_secret_pick(secret: Dict[str, Any], *keys: str) -> Optional[str]:
    for k in keys:
        v = secret.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def _snowflake_jdbc_host(secret: Dict[str, Any]) -> str:
    """
    Prefer sfURL (e.g. https://xyz.us-east-1.aws.snowflakecomputing.com); else sfAccount / legacy account.
    """
    sf_url = _sf_secret_pick(secret, "sfURL", "sf_url", "SFURL")
    if sf_url:
        h = sf_url.replace("https://", "").replace("http://", "").strip().split("/")[0]
        if h:
            return h
    acct = _sf_secret_pick(
        secret,
        "sfAccount",
        "sf_account",
        "account",
        "ACCOUNT",
    )
    if not acct:
        raise ValueError("Snowflake secret must include sfURL or sfAccount (or legacy account)")
    if ".snowflakecomputing.com" in acct:
        return acct
    return f"{acct}.snowflakecomputing.com"


def _snowflake_jdbc_url_and_props(secret: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
    """
    Supports Secrets Manager JSON with NBCU-style keys (sfUser, sfDatabase, sfSchema, sfWarehouse,
    sfRole, sfURL / sfAccount, pem_private_key, sfPassphrase) or legacy keys (user, password, …).

    Key-pair auth uses JDBC private_key_base64 + authenticator=snowflake_jwt (Snowflake JDBC 3.14+).
    """
    user = _sf_secret_pick(
        secret,
        "sfUser",
        "sf_user",
        "user",
        "username",
        "USER",
        "USERNAME",
    )
    if not user:
        raise ValueError("Snowflake secret must include sfUser or user")

    warehouse = _sf_secret_pick(secret, "sfWarehouse", "sf_warehouse", "warehouse", "WAREHOUSE")
    database = _sf_secret_pick(secret, "sfDatabase", "sf_database", "database", "DATABASE")
    schema = _sf_secret_pick(secret, "sfSchema", "sf_schema", "schema", "SCHEMA") or "PUBLIC"
    role = _sf_secret_pick(secret, "sfRole", "sf_role", "role", "ROLE")

    password = _sf_secret_pick(
        secret,
        "password",
        "PASSWORD",
        "sfPassword",
        "sf_password",
    )
    pem_b64 = _sf_secret_pick(secret, "pem_private_key", "private_key_pem", "PEM_PRIVATE_KEY")
    passphrase = _sf_secret_pick(secret, "sfPassphrase", "sf_passphrase", "private_key_file_pwd", "SF_PASSPHRASE")

    host = _snowflake_jdbc_host(secret)
    url = f"jdbc:snowflake://{host}/"
    params: List[str] = []
    if warehouse:
        params.append(f"warehouse={quote_plus(warehouse)}")
    if database:
        params.append(f"db={quote_plus(database)}")
    if schema:
        params.append(f"schema={quote_plus(schema)}")
    if role:
        params.append(f"role={quote_plus(role)}")
    if params:
        url = url + "?" + "&".join(params)

    props: Dict[str, str] = {
        "user": user,
        "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
    }

    if pem_b64:
        try:
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization
        except ImportError as ex:
            raise ImportError(
                "Snowflake key-pair auth requires the cryptography package on the Glue job classpath."
            ) from ex

        pem_str = pem_b64.replace("\\n", "\n").strip()
        if not pem_str.startswith("-----BEGIN"):
            raise ValueError("pem_private_key must be a PEM string (BEGIN … PRIVATE KEY)")

        pem_bytes = pem_str.encode("utf-8")
        pass_bytes = passphrase.encode("utf-8") if passphrase else None
        pkey = serialization.load_pem_private_key(pem_bytes, password=pass_bytes, backend=default_backend())
        pkcs8_der = pkey.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        props["private_key_base64"] = base64.standard_b64encode(pkcs8_der).decode("utf-8")
        props["authenticator"] = "snowflake_jwt"
    elif password:
        props["password"] = password
    else:
        raise ValueError(
            "Snowflake secret must include pem_private_key (and optional sfPassphrase) for JWT auth, "
            "or password / sfPassword for password auth."
        )

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


def _write_reverse_input_and_ctrl_tables(
    spark: SparkSession,
    df_batch,
    batch_id: str,
    secret_arn: str,
    input_table_fqn: str,
    ctrl_table_fqn: str,
):
    """
    A) Write one request row per candidate to reverse input table.
    B) Write one control row per batch to reverse control table with STATUS='NEW'.
    """
    if not secret_arn or secret_arn.strip().upper() in ("", "DISABLED", "NONE", "NOT_CONFIGURED"):
        logger.warning("Snowflake secret missing; skipping reverse input/control table writes.")
        return

    secret = _secrets_get_json(secret_arn)
    jdbc_url, props = _snowflake_jdbc_url_and_props(secret)

    if df_batch.rdd.isEmpty():
        logger.info("Skipping reverse input/control JDBC writes: empty dataframe.")
        return

    # Build request payload rows
    req_df = df_batch.withColumn("BATCH_ID", lit(batch_id))
    req_df = req_df.withColumn(
        "RECORD_ID",
        coalesce(
            col("batch_record_id").cast(StringType()) if "batch_record_id" in req_df.columns else lit(None).cast(StringType()),
            concat_ws(":", coalesce(col("source_name"), lit("")), coalesce(col("source_customer_id"), lit(""))),
        ),
    )
    req_df = req_df.withColumn("INPUT_EMAIL_ADDRESS", col(EMAIL_KEY_COL))
    req_df = req_df.withColumn("INPUT_PHONE", when(col("ml_phone").isNotNull(), trim(col("ml_phone"))).otherwise(lit(None).cast(StringType())))
    # Uppercase hex SHA-256 to match DynamoDB email_hash / NBCU reverse-append exports
    req_df = req_df.withColumn(
        "INPUT_EMAIL_HASH",
        when(col(EMAIL_HASH_COL).isNotNull(), col(EMAIL_HASH_COL)).otherwise(
            when(col(EMAIL_KEY_COL).isNotNull(), upper(sha2(col(EMAIL_KEY_COL), 256))).otherwise(lit(None).cast(StringType()))
        ),
    )
    req_df = req_df.withColumn("INPUT_PHONE_HASH", when(col("INPUT_PHONE").isNotNull(), sha2(col("INPUT_PHONE"), 256)).otherwise(lit(None).cast(StringType())))
    req_df = req_df.select("BATCH_ID", "RECORD_ID", "INPUT_EMAIL_ADDRESS", "INPUT_PHONE", "INPUT_EMAIL_HASH", "INPUT_PHONE_HASH")

    req_df.write.jdbc(url=jdbc_url, table=input_table_fqn, mode="append", properties=props)

    # Build one control row for this batch
    ctrl_df = spark.range(1).select(
        lit(batch_id).alias("BATCH_ID"),
        lit("NEW").alias("STATUS"),
        current_timestamp().alias("CREATED_TS"),
        current_timestamp().alias("UPDATED_TS"),
    )
    ctrl_df.write.jdbc(url=jdbc_url, table=ctrl_table_fqn, mode="append", properties=props)


def _pick_value_case_insensitive(row_dict: Dict[str, Any], candidates: List[str]):
    lower_map = {str(k).lower(): v for k, v in row_dict.items()}
    for c in candidates:
        if c.lower() in lower_map and lower_map[c.lower()] is not None:
            return lower_map[c.lower()]
    return None


def _payload_from_reverse_output_row(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map reverse output table columns to common-schema ml_* enrichment payload.
    """
    return {
        "ml_first_name": _pick_value_case_insensitive(row_dict, ["ml_first_name", "first_name", "name"]),
        "ml_last_name": _pick_value_case_insensitive(row_dict, ["ml_last_name", "last_name", "surname"]),
        "ml_address_line_1": _pick_value_case_insensitive(row_dict, ["ml_address_line_1", "address_line_1", "address1", "address"]),
        "ml_address_line_2": _pick_value_case_insensitive(row_dict, ["ml_address_line_2", "address_line_2", "address2"]),
        "ml_city": _pick_value_case_insensitive(row_dict, ["ml_city", "city"]),
        "ml_state_code": _pick_value_case_insensitive(row_dict, ["ml_state_code", "state_code", "state", "province"]),
        "ml_postal_code": _pick_value_case_insensitive(row_dict, ["ml_postal_code", "postal_code", "zipcode", "zip"]),
        "ml_country_code": _pick_value_case_insensitive(row_dict, ["ml_country_code", "country_code", "country"]),
        "ml_phone": _pick_value_case_insensitive(row_dict, ["ml_phone", "phone", "phone_number"]),
    }


def _snowflake_fetch_from_reverse_output(
    spark: SparkSession,
    secret_arn: str,
    output_table_fqn: str,
    batch_id: str,
    max_wait_seconds: int = 300,
    poll_seconds: int = 10,
) -> Dict[str, Dict[str, Any]]:
    """
    Read enrichment rows from reverse output table for a given batch_id.
    Returns a map keyed by normalized email_address -> payload.
    """
    if not output_table_fqn or output_table_fqn.strip().upper() in ("", "DISABLED", "NONE", "NOT_CONFIGURED"):
        return {}

    secret = _secrets_get_json(secret_arn)
    jdbc_url, props = _snowflake_jdbc_url_and_props(secret)

    deadline = time.time() + max_wait_seconds
    rows = []
    while time.time() <= deadline:
        safe_bid = batch_id.replace("'", "''")
        query = f"(SELECT * FROM {output_table_fqn} WHERE BATCH_ID = '{safe_bid}') AS reverse_output_sub"
        df_out = spark.read.jdbc(url=jdbc_url, table=query, properties=props)
        if not df_out.rdd.isEmpty():
            rows = df_out.collect()
            break
        time.sleep(poll_seconds)

    if not rows:
        logger.warning("No rows found in reverse output table for batch_id=%s within %s seconds.", batch_id, max_wait_seconds)
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        rd = row.asDict()
        email_raw = _pick_value_case_insensitive(
            rd,
            ["INPUT_EMAIL_ADDRESS", "input_email_address", "email", "email_address", "ml_email_address"],
        )
        if email_raw is None:
            continue
        email_key = str(email_raw).strip().lower()
        if not email_key:
            continue
        payload = _payload_from_reverse_output_row(rd)
        payload = {k: v for k, v in payload.items() if v is not None and str(v).strip() != ""}
        if payload:
            out[email_key] = payload
    return out


def run_reverse_append_preflight(df_batch, spark: SparkSession, job_args: Dict[str, Any]):
    """
    Rule 600 → email_hash → DynamoDB (email_hash PK, flat attributes per export schema).
    Found rows: merge ml_* from DDB. Not found: Snowflake reverse path only for misses → write flat items to DDB → merge.
    Adds reverse_append_lookup_status: Found in DDB | NotFound But Added | NotFound & Not Added.
    """

    if not _preflight_enabled(job_args.get("reverse_append_preflight_enabled", "false")):
        return df_batch

    cache_table = (job_args.get("reverse_append_cache_table") or "").strip()
    secret_arn = (job_args.get("snowflake_jdbc_secret_arn") or "").strip()
    table_fqn = (job_args.get("snowflake_enrichment_table_fqn") or "").strip()
    email_column = (job_args.get("snowflake_email_column") or "EMAIL").strip()
    reverse_input_table_fqn = _optional_arg("snowflake_reverse_input_table_fqn", DEFAULT_REVERSE_INPUT_TABLE).strip()
    reverse_ctrl_table_fqn = _optional_arg("snowflake_reverse_ctrl_table_fqn", DEFAULT_REVERSE_CTRL_TABLE).strip()
    reverse_output_table_fqn = _optional_arg("snowflake_reverse_output_table_fqn", DEFAULT_REVERSE_OUTPUT_TABLE).strip()
    reverse_output_wait_seconds = int(_optional_arg("snowflake_reverse_output_wait_seconds", "300"))
    reverse_output_poll_seconds = int(_optional_arg("snowflake_reverse_output_poll_seconds", "10"))
    source_ds = _optional_arg("reverse_append_source_ds", "universal_customer_mesh").strip() or "universal_customer_mesh"

    if EMAIL_COL not in df_batch.columns:
        log = logging.getLogger(__name__)
        log.warning("Column %s missing; skipping reverse-append preflight.", EMAIL_COL)
        return df_batch

    intermediate_s3_output_path = f"s3://{job_args['incremental_data_s3_bucket']}/intermediate_data/"
    debug_output_s3 = job_args.get("debug_output_s3", "false")
    df_batch = Rule_0600_EmailCleaner.apply_rule(
        df_batch,
        "reverse_append_preflight",
        intermediate_s3_output_path,
        debug_output_s3,
    )

    df_batch = df_batch.withColumn(
        EMAIL_KEY_COL,
        when(
            col(EMAIL_COL).isNull() | (trim(col(EMAIL_COL)) == lit("")),
            lit(None).cast(StringType()),
        ).otherwise(lower(trim(col(EMAIL_COL)))),
    )
    df_batch = df_batch.withColumn(
        EMAIL_HASH_COL,
        when(col(EMAIL_KEY_COL).isNotNull(), upper(sha2(col(EMAIL_KEY_COL), 256))).otherwise(lit(None).cast(StringType())),
    )

    log = logging.getLogger(__name__)

    if not cache_table or cache_table.lower() in ("disabled", "none"):
        log.warning("Preflight enabled but reverse_append_cache_table missing; skipping DDB/SF enrichment.")
        out = df_batch.withColumn(STATUS_COL, lit(None).cast(StringType()))
        return out.drop(EMAIL_KEY_COL, EMAIL_HASH_COL)

    keys_df = df_batch.where(col(EMAIL_KEY_COL).isNotNull()).select(EMAIL_KEY_COL, EMAIL_HASH_COL).distinct()
    pairs = keys_df.collect()
    email_keys = [r[0] for r in pairs if r[0]]
    all_hashes = [str(r[1]).upper() for r in pairs if r[1]]
    hash_to_email = {str(r[1]).upper(): r[0] for r in pairs if r[0] and r[1]}

    if not email_keys:
        out = df_batch.withColumn(STATUS_COL, lit(None).cast(StringType()))
        return out.drop(EMAIL_KEY_COL, EMAIL_HASH_COL)

    from_ddb_plain = _ddb_batch_get_by_email_hash(cache_table, all_hashes)
    ddb_hit_hashes = set(from_ddb_plain.keys())
    missing_hashes = [h for h in all_hashes if h not in ddb_hit_hashes]
    missing_emails = [hash_to_email[h] for h in missing_hashes if h in hash_to_email]

    enrich_by_email: Dict[str, Dict[str, Any]] = {}
    for h, plain in from_ddb_plain.items():
        ml = _ddb_flat_row_to_ml_payload(plain)
        if not ml:
            continue
        addr = plain.get("email_address")
        ek = str(addr).strip().lower() if addr is not None else hash_to_email.get(h.upper())
        if ek:
            enrich_by_email[ek] = ml

    from_sf: Dict[str, Dict[str, Any]] = {}
    preflight_batch_id = ""
    if missing_emails and secret_arn.upper() not in ("DISABLED", "NONE", "") and table_fqn.upper() not in (
        "NOT_CONFIGURED",
        "",
        "NONE",
    ):
        preflight_batch_id = datetime.now(pytz.UTC).strftime("%Y%m%d%H%M%S%f")
        try:
            df_sf_in = df_batch.where(col(EMAIL_HASH_COL).isin(missing_hashes))
            _write_reverse_input_and_ctrl_tables(
                spark=spark,
                df_batch=df_sf_in,
                batch_id=preflight_batch_id,
                secret_arn=secret_arn,
                input_table_fqn=reverse_input_table_fqn,
                ctrl_table_fqn=reverse_ctrl_table_fqn,
            )
            from_sf = _snowflake_fetch_from_reverse_output(
                spark=spark,
                secret_arn=secret_arn,
                output_table_fqn=reverse_output_table_fqn,
                batch_id=preflight_batch_id,
                max_wait_seconds=reverse_output_wait_seconds,
                poll_seconds=reverse_output_poll_seconds,
            )
            if not from_sf:
                from_sf = _snowflake_fetch_by_emails(spark, secret_arn, table_fqn, email_column, missing_emails)

            ddb_items: List[Dict[str, Any]] = []
            for em, ml in from_sf.items():
                if not ml:
                    continue
                h_upper = hashlib.sha256(em.encode("utf-8")).hexdigest().upper()
                ddb_items.append(_ml_payload_to_reverse_append_item(em, h_upper, ml, preflight_batch_id, source_ds))
            _ddb_put_reverse_append_items(cache_table, ddb_items)
        except Exception as ex:
            log.error("Snowflake reverse-append path failed: %s", ex)
            raise
    elif missing_emails:
        log.warning(
            "%d emails not in DynamoDB; Snowflake not configured — proceeding without reverse-append enrichment.",
            len(missing_emails),
        )

    for em, ml in from_sf.items():
        if ml:
            enrich_by_email[em] = ml

    if ddb_hit_hashes:
        ddb_hit_df = spark.createDataFrame([(h,) for h in ddb_hit_hashes], [EMAIL_HASH_COL]).withColumn(
            "__ddb_hit", lit(True)
        )
        df_batch = df_batch.join(ddb_hit_df, EMAIL_HASH_COL, "left")
    else:
        df_batch = df_batch.withColumn("__ddb_hit", lit(None).cast(BooleanType()))

    df_batch = df_batch.withColumn("__ddb_hit", coalesce(col("__ddb_hit"), lit(False)))

    if from_sf:
        sf_hit_df = spark.createDataFrame([(e,) for e in from_sf.keys()], [EMAIL_KEY_COL]).withColumn(
            "__sf_added", lit(True)
        )
        df_batch = df_batch.join(sf_hit_df, EMAIL_KEY_COL, "left")
    else:
        df_batch = df_batch.withColumn("__sf_added", lit(None).cast(BooleanType()))

    df_batch = df_batch.withColumn("__sf_added", coalesce(col("__sf_added"), lit(False)))

    if enrich_by_email:
        rows = [(k, json.dumps(v)) for k, v in enrich_by_email.items()]
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
    else:
        out = df_batch.withColumn("__enrich_json", lit(None).cast(StringType()))

    out = out.withColumn(
        STATUS_COL,
        when(col(EMAIL_KEY_COL).isNull(), lit(None).cast(StringType()))
        .when(col("__ddb_hit"), lit(FOUND_IN_DDB))
        .when(col("__sf_added"), lit(NOTFOUND_BUT_ADDED))
        .when(col(EMAIL_KEY_COL).isNotNull(), lit(NOTFOUND_NOT_ADDED))
        .otherwise(lit(None).cast(StringType())),
    )

    drop_cols = [EMAIL_KEY_COL, EMAIL_HASH_COL, "__ddb_hit", "__sf_added"]
    for c in ("__enrich_json", "__enrich_struct"):
        if c in out.columns:
            drop_cols.append(c)
    return out.drop(*[c for c in drop_cols if c in out.columns])


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

            _write_rea_merged_debug_parquet(df_batch, _rea_merged_debug_s3_path())

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
