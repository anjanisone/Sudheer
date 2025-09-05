from rules.abstract_rule import AbstractRule

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import uuid


# ---------- Utilities ----------
def _gen_uuid() -> str:
    return str(uuid.uuid4())

gen_uuid_udf = F.udf(_gen_uuid, StringType())


class Rule_0200_emailonly_datasource(AbstractRule):
    """
    Behavior:
      - For 'email-only' records (email present, other core fields empty) that have NO uxid:
          * If the email already exists on any record with uxid -> REUSE that uxid
          * Else -> CREATE a new uxid (UUID) for that email and assign
          * Log each assignment (CREATED/REUSED) with email, final_uxid, id_res_job_id, timestamp

    Assumes presence of:
      ml_email_address (string)
      uxid (string, nullable)
      id_res_last_updated_timestamp (string, ISO-8601 UTC)
      Optional fields (used only to detect "email-only"):
        ml_first_name, ml_middle_name, ml_last_name,
        ml_address_line_1, ml_address_line_2,
        ml_birth_date, ml_phone, rules_guest_id
    """

    def apply_rule(self, df_batch, id_res_job_id: str, intermediate_s3_output_path: str, debug_output_s3: str):
        # If there's no email column, nothing to do
        if "ml_email_address" not in df_batch.columns:
            return df_batch

        df = df_batch

        # Normalize email
        df = df.withColumn("ml_email_address", F.trim(F.col("ml_email_address")))

        # Presence helpers (robust if columns are missing)
        def pres(cname: str):
            return (
                F.when(
                    F.col(cname).isNotNull() & (F.length(F.trim(F.col(cname))) > 0),
                    F.lit(1),
                )
                .otherwise(F.lit(0))
                .alias(f"pres_{cname}")
                if cname in df.columns
                else F.lit(0).alias(f"pres_{cname}")
            )

        presence_flags = [
            pres("ml_first_name"),
            pres("ml_middle_name"),
            pres("ml_last_name"),
            pres("ml_address_line_1"),
            pres("ml_address_line_2"),
            pres("ml_birth_date"),
            pres("ml_phone"),
            pres("rules_guest_id"),
        ]
        df = df.select("*", *presence_flags)

        email_present = F.when(
            F.col("ml_email_address").isNotNull() & (F.length(F.col("ml_email_address")) > 0),
            F.lit(1),
        ).otherwise(F.lit(0))
        non_email_any = (
            F.col("pres_ml_first_name")
            + F.col("pres_ml_middle_name")
            + F.col("pres_ml_last_name")
            + F.col("pres_ml_address_line_1")
            + F.col("pres_ml_address_line_2")
            + F.col("pres_ml_birth_date")
            + F.col("pres_ml_phone")
            + F.col("pres_rules_guest_id")
        )

        # Email-only marker
        df = df.withColumn("tmp_email_only_record", (email_present == 1) & (non_email_any == 0))
        # Needs-uxid marker
        df = df.withColumn("needs_uxid", (F.col("tmp_email_only_record") == True) & (F.col("uxid").isNull()))

        # Map existing uxid by email (from any rows that already have one)
        existing_uxid_by_email = (
            df.filter(F.col("uxid").isNotNull())
              .groupBy("ml_email_address")
              .agg(F.first("uxid", ignorenulls=True).alias("existing_uxid"))
        )
        df = df.join(existing_uxid_by_email, on=["ml_email_address"], how="left")

        # For emails that need a uxid and have no existing mapping, create one UUID per distinct email
        new_email_df = (
            df.filter((F.col("needs_uxid") == True) & F.col("existing_uxid").isNull())
              .select("ml_email_address")
              .distinct()
              .withColumn("new_uxid", gen_uuid_udf())
        )
        df = df.join(new_email_df, on=["ml_email_address"], how="left")

        # Final resolution
        df = df.withColumn(
            "uxid",
            F.when(
                F.col("needs_uxid") == True,
                F.when(F.col("existing_uxid").isNotNull(), F.col("existing_uxid")).otherwise(F.col("new_uxid")),
            ).otherwise(F.col("uxid")),
        )

        # Touch last_updated when we assign
        df = df.withColumn(
            "id_res_last_updated_timestamp",
            F.when(
                F.col("needs_uxid") == True,
                F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
            ).otherwise(F.col("id_res_last_updated_timestamp")),
        )

        # Logging (CREATED / REUSED)
        action_col = (
            F.when(F.col("needs_uxid") == True,
                   F.when(F.col("existing_uxid").isNotNull(), F.lit("REUSED")).otherwise(F.lit("CREATED")))
             .otherwise(F.lit(None))
             .alias("action")
        )

        log_df = (
            df.select(
                F.lit(id_res_job_id).alias("id_res_job_id"),
                F.col("ml_email_address").alias("email"),
                F.col("uxid").alias("final_uxid"),
                action_col,
                F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("logged_at_utc"),
            )
            .filter(F.col("action").isNotNull())
            .distinct()
        )

        if debug_output_s3:
            log_path = debug_output_s3.rstrip("/") + "/rule_0200_email_assignment"
            (log_df.repartition(1).write.mode("append").format("json").save(log_path))

        # Cleanup temps
        drop_cols = [c for c in df.columns if c.startswith("pres_")] + [
            "tmp_email_only_record",
            "needs_uxid",
            "existing_uxid",
            "new_uxid",
        ]
        for c in drop_cols:
            if c in df.columns:
                df = df.drop(c)

        # Optional checkpoint (kept here in case your pipeline expects it)
        # df = df.checkpoint(eager=True)
        # _ = df.count()

        return df
