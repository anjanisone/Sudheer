from rules.abstract_rule import AbstractRule

from pyspark.sql import Window
from pyspark.sql.functions import col, first, when, lit, current_timestamp, date_format, udf, row_number
from pyspark.sql.types import StringType, DoubleType
from datetime import datetime
import uuid
from pyspark.storagelevel import StorageLevel
from graphframes import GraphFrame

class Rule_0150_GuestIdRule(AbstractRule):
    """
    If two records have the same guest_id, then it should have the same uxid,
    regardless of what ER's ML results are.

    Updated: also unify different guest_ids that share the same email into the
    same UXID (email fallback step inside Rule_150).
    """

    def apply_rule(df_batch, id_res_job_id: str, intermediate_s3_output_path: str, debug_output_s3: str):
        print('Rule_0150_GuestIdRule')

        df_batch = df_batch.checkpoint(eager=True)

        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
        df_uuid_convert = df_batch.select('rules_guest_id').distinct().withColumn("new_uxid", uuid_udf()).persist(StorageLevel.DISK_ONLY)
        df_batch = df_batch.join(df_uuid_convert, 'rules_guest_id', 'left')

        # NEW: email fallback – reuse existing UXID by email if present
        email_win = Window.partitionBy('email').orderBy(
            col('id_res_last_updated_timestamp').asc_nulls_last(),
            col('uxid_score').desc_nulls_last(),
            col('batch_record_id').asc_nulls_last()
        )
        email_map = (
            df_batch.filter(col('email').isNotNull() & col('uxid').isNotNull())
                    .withColumn('_er', row_number().over(email_win))
                    .filter(col('_er') == 1)
                    .select('email', col('uxid').alias('_email_uxid'))
        )
        df_batch = df_batch.join(email_map, 'email', 'left')

        # NEW: modify simple_uxid assignment to use email_uxid when guest_id had no UXID
        df_batch = df_batch.withColumn(
            'simple_uxid',
            when(
                col('rules_guest_id').isNotNull() & col('uxid').isNull() & col('_email_uxid').isNotNull(),
                col('_email_uxid')
            ).when(
                col('rules_guest_id').isNotNull() & col('uxid').isNull(),
                col('new_uxid')
            ).otherwise(col('uxid'))
        )
        df_batch = df_batch.drop('_email_uxid')

        df_batch = df_batch.withColumn(
            'uxid_confidence',
            when(col('rules_guest_id').isNotNull() & col('uxid').isNull(), lit(0.0).cast(DoubleType())).otherwise(col('uxid_confidence'))
        )

        condition_data_for_graph = col('rules_guest_id').isNotNull() & col('simple_uxid').isNotNull()
        df_data_for_graph_edges = df_batch.select('rules_guest_id', 'simple_uxid').filter(condition_data_for_graph).distinct().orderBy('rules_guest_id')

        df_edges = df_data_for_graph_edges.select(
            col('rules_guest_id'), col('simple_uxid'),
            Window.partitionBy('rules_guest_id').orderBy('rules_guest_id')
        )
        df_edges = df_edges.withColumnRenamed("simple_uxid", "src")
        df_edges = df_edges.withColumnRenamed("lag(simple_uxid) OVER (PARTITION BY rules_guest_id ORDER BY rules_guest_id ASC NULLS FIRST)", "dst")
        df_edges = df_edges.filter(col('dst').isNotNull()) \
                        .select(
                            col('rules_guest_id').alias('rules_guest_id_edges'), col('src'), col('dst')
                        )

        df_vertices = df_batch.select('simple_uxid').filter(col('simple_uxid').isNotNull()).distinct()
        df_vertices = df_vertices.withColumn('id', col('simple_uxid'))

        gf = GraphFrame(df_vertices, df_edges)
        df_guest_id_graph_comp = gf.connectedComponents().select('id', 'component')
        df_guest_id_graph_comp = df_guest_id_graph_comp.withColumnRenamed('id', 'simple_uxid')
        df_guest_id_graph_comp = df_guest_id_graph_comp.withColumn('rules_guest_id_graph_component', col('component'))

        df_temp = df_guest_id_graph_comp.select('simple_uxid', 'component')
        df_temp_a = df_temp.alias("a")
        df_temp_b = df_temp.alias("b")
        df_same_one_uxid_in_multiple_graphs = df_temp_a.join(
            df_temp_b,
            (col("a.simple_uxid") == col("b.simple_uxid")) & (col("a.component") != col("b.component"))
        )
        if df_same_one_uxid_in_multiple_graphs.count() > 0:
            raise Exception('The same simple_uxid is in multiple guest_id graph components.')

        df_batch = df_batch.join(df_guest_id_graph_comp, 'simple_uxid', 'left')

        window_spec = Window.partitionBy('rules_guest_id_graph_component').orderBy('rules_guest_id')
        df_batch_component_ordered_results = (
            df_batch.select(
                'rules_guest_id_graph_component',
                'rules_guest_id', 'uxid', 'simple_uxid',
                'source_name', 'source_id'
            )
            .orderBy('rules_guest_id_graph_component', 'rules_guest_id')
        )

        df_batch_component_not_null_results = df_batch_component_ordered_results.filter('rules_guest_id_graph_component is not null')
        df_batch_component_null_results = df_batch_component_ordered_results.filter('rules_guest_id_graph_component is null')

        df_batch_component_not_null_results = df_batch_component_not_null_results.withColumn(
            'final_uxid',
            first('simple_uxid', ignorenulls=True).over(window_spec)
        )

        df_batch_component_not_null_results = df_batch_component_not_null_results.withColumn(
            'final_confidence',
            when(col('rules_guest_id_graph_component').isNotNull() & col('rules_guest_id').isNotNull(), lit(0.99).cast(DoubleType()))
            .otherwise(lit(0.0).cast(DoubleType()))
        )

        df_batch_component_null_results = df_batch_component_null_results.withColumn('final_uxid', col('simple_uxid'))

        df_batch_component_null_results = df_batch_component_null_results.withColumn(
            'final_confidence',
            when(col('final_uxid').isNotNull(), lit(0.99).cast(DoubleType())).otherwise(lit(0.0).cast(DoubleType()))
        )

        df_batch_results = df_batch_component_not_null_results.unionByName(df_batch_component_null_results)

        df_batch_results = df_batch_results.withColumn('id_res_job_id', lit(id_res_job_id))
        df_batch_results = df_batch_results.withColumn('id_res_batch_id', lit(''))
        df_batch_results = df_batch_results.withColumn('id_res_batch_name', lit(''))
        df_batch_results = df_batch_results.withColumn('write_timestamp', current_timestamp())
        df_batch_results = df_batch_results.withColumn('write_timestamp_str', date_format(col('write_timestamp'), "yyyyMMddHHmmss"))

        df_batch_results = df_batch_results.drop('new_uxid', 'component', 'simple_uxid')

        df_batch_results = df_batch_results.select(
            'rules_guest_id_graph_component', 'rules_guest_id', 'final_uxid', 'final_confidence',
            'id_res_job_id', 'id_res_batch_id', 'id_res_batch_name', 'write_timestamp', 'write_timestamp_str'
        )
        df_batch = df_batch.alias('a').join(df_batch_results.alias('b'), col('a.rules_guest_id') == col('b.rules_guest_id'), 'left')\
            .select('a.*', col('b.final_uxid').alias('uxid'), col('b.final_confidence').alias('uxid_confidence'),
                    col('b.id_res_job_id'), col('b.id_res_batch_id'), col('b.id_res_batch_name'),
                    col('b.write_timestamp'), col('b.write_timestamp_str'))

        df_batch = df_batch.checkpoint(eager=True)
        print('Combine results dataframes...Done')
        print('')

        return df_batch_results
