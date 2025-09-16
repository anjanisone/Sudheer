from rules.abstract_rule import AbstractRule

from pyspark.sql import Window
from pyspark.sql.functions import (
    col, first, when, lag, lit, current_timestamp, date_format, udf, countDistinct
)
from pyspark.sql.types import StringType, DoubleType
from datetime import datetime
import uuid
from pyspark.storagelevel import StorageLevel


class Rule_0170_GuestIdRule(AbstractRule):
    """
    Group by email. If the same email has different guest_id values, choose the oldest
    record in that email group by id_res_last_updated_timestamp and take its UXID, then
    assign that UXID to all records of the email group.

    If there is only one guest_id in the grouped email records, use the uxid associated
    with that guest_id and assign it to all the records of that email group.
    """

    def apply_rule(df_batch, id_res_job_id: str, intermediate_s3_output_path: str, debug_output_s3: str):
        print('Rule_0170_GuestIdRule')

        # Checkpoint to avoid re-computation inconsistencies further down the pipeline (same pattern as Rule_0150)
        df_batch = df_batch.checkpoint(eager=True)

        # Create simple_uxid column similar to Rule_0150 logic:
        # - If a record has a rules_guest_id but no uxid yet, create a new uuid as simple_uxid
        # - Else, simple_uxid is the existing uxid
        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
        df_uuid_convert = (
            df_batch.select('rules_guest_id')
            .distinct()
            .withColumn("new_uxid", uuid_udf())
            .persist(StorageLevel.DISK_ONLY)
        )
        df_batch = df_batch.join(df_uuid_convert, 'rules_guest_id', 'left')

        df_batch = df_batch.withColumn(
            'simple_uxid',
            when(col('rules_guest_id').isNotNull() & col('uxid').isNull(), col('new_uxid')).otherwise(col('uxid'))
        )
        df_batch = df_batch.withColumn(
            'uxid_confidence',
            when(col('rules_guest_id').isNotNull() & col('uxid').isNull(), lit(1.0).cast(DoubleType())).otherwise(col('uxid_confidence'))
        )
        df_batch = df_batch.withColumn(
            'id_res_last_updated_timestamp',
            when(
                col('rules_guest_id').isNotNull() & col('uxid').isNull(),
                lit(date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
            ).otherwise(col('id_res_last_updated_timestamp'))
        )

        # Persist the working set used for grouping
        df_batch = df_batch.persist(StorageLevel.DISK_ONLY)
        df_batch.count()

        # Compute how many distinct guest_ids exist per email group
        # (We will always choose from the "oldest" record per email; this satisfies
        # both the multi-guest_id and single-guest_id cases described.)
        print('Calculate distinct guest_id count per email...')
        df_email_guest_counts = (
            df_batch
            .select('email', 'rules_guest_id')
            .filter(col('email').isNotNull())
            .distinct()
            .groupBy('email')
            .agg(countDistinct('rules_guest_id').alias('guest_id_count'))
            .persist(StorageLevel.DISK_ONLY)
        )
        df_email_guest_counts.count()
        print('Calculate distinct guest_id count per email...Done')
        print('')

        # Join guest_id_count back to the batch
        df_batch = df_batch.join(df_email_guest_counts, 'email', 'left')

        # Window to pick the OLDEST record within each email group
        # Primary sort: id_res_last_updated_timestamp ascending (oldest first, nulls last)
        # Tie-breakers: higher uxid_confidence first, then deterministic by batch_record_id
        window_spec_email = Window.partitionBy('email').orderBy(
            col('id_res_last_updated_timestamp').asc_nulls_last(),
            col('uxid_confidence').cast(DoubleType()).desc_nulls_last(),
            col('batch_record_id').asc_nulls_last()
        )

        print('Decide uxid per email group based on the oldest record...')
        # For each email group, pick the oldest record’s simple_uxid, uxid_confidence, and id_res_last_updated_timestamp
        df_result = (
            df_batch
            .withColumn("chosen_uxid", first('simple_uxid', ignorenulls=True).over(window_spec_email))
            .withColumn("chosen_conf", first('uxid_confidence', ignorenulls=True).over(window_spec_email))
            .withColumn("chosen_updated_ts", first('id_res_last_updated_timestamp', ignorenulls=True).over(window_spec_email))
        )

        # Sanity checks: after choosing, these should not be null for any row that had an email
        if df_result.filter(col('email').isNotNull() & col('chosen_uxid').isNull()).count() != 0:
            error_msg = 'ERRORERROR: chosen_uxid is null for some email groups; this is not expected.'
            print(error_msg)
            raise ValueError(error_msg)
        if df_result.filter(col('email').isNotNull() & col('chosen_conf').isNull()).count() != 0:
            error_msg = 'ERRORERROR: chosen_conf is null for some email groups; this is not expected.'
            print(error_msg)
            raise ValueError(error_msg)
        if df_result.filter(col('email').isNotNull() & col('chosen_updated_ts').isNull()).count() != 0:
            error_msg = 'ERRORERROR: chosen_updated_ts is null for some email groups; this is not expected.'
            print(error_msg)
            raise ValueError(error_msg)

        # Assign the chosen values to all rows in the email group
        df_result = (
            df_result
            .withColumn('uxid', col('chosen_uxid'))
            .withColumn('uxid_confidence', col('chosen_conf'))
            .withColumn('id_res_last_updated_timestamp', col('chosen_updated_ts'))
        )

        print('Decide uxid per email group based on the oldest record...Done')
        print('')

        # Clean up temporary columns
        df_result = df_result.drop('new_uxid', 'simple_uxid', 'guest_id_count', 'chosen_uxid', 'chosen_conf', 'chosen_updated_ts')

        # Checkpoint at the end, mirroring the pattern in Rule_0150
        df_result = df_result.checkpoint(eager=True)

        return df_result
