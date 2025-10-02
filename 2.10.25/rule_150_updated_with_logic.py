from rules.abstract_rule import AbstractRule
from pyspark.sql import Window
from pyspark.sql.functions import col, first, when, lag, lit, current_timestamp, date_format, udf, row_number
from pyspark.sql.types import StringType, DoubleType
from datetime import datetime
import uuid
from pyspark.storagelevel import StorageLevel
from graphframes import GraphFrame

class Rule_0150_GuestIdRule(AbstractRule):
    """
    If two records have the same guest_id, then it should have the same uxid, regardless of what ER's ML results are
    """

    def apply_rule(df_batch, id_res_job_id: str, intermediate_s3_output_path: str, debug_output_s3: str):
        print('Rule_0150_GuestIdRule')

        # The graphframes library used in this python file uses checkpointing, which triggers recalculation even for persisted datasets and leads to inconsistencies in the uxid's assigned in this python file.
        # So, checkpointing df_batch to ensure that the triggered recalculations use this point in the code as a starting point instead of going back to an earlier already executed rule
        df_batch = df_batch.checkpoint(eager=True)

        # Create simple_uxid column. This uxid column is the uxid value from the previous rule's AssignUxid logic based on ER service MatchID results.
        # Additionally, if a record has a rules_guest_id but no MatchID results, then a new uxid is created for this simple_uxid value
        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
        df_uuid_convert = df_batch.select('rules_guest_id').distinct().withColumn("new_uxid", uuid_udf()).persist(StorageLevel.DISK_ONLY)
        df_batch = df_batch.join(df_uuid_convert, 'rules_guest_id', 'left')
        df_batch = df_batch.withColumn('simple_uxid',
            when(col('rules_guest_id').isNotNull() & col('uxid').isNull(), col('new_uxid')).otherwise(col('uxid'))
        )
        df_batch = df_batch.withColumn('uxid_confidence',
            when(col('rules_guest_id').isNotNull() & col('uxid').isNull(), lit(1.0).cast(DoubleType())).otherwise(col('uxid_confidence'))
        )
        df_batch = df_batch.withColumn('id_res_last_updated_timestamp',
            when(col('rules_guest_id').isNotNull() & col('uxid').isNull(), lit(date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))).otherwise(col('id_res_last_updated_timestamp'))
        )

        # Email fallback logic: reuse an existing UXID for records with the same email
        window_email = Window.partitionBy('email').orderBy(
            col('id_res_last_updated_timestamp').asc_nulls_last(),
            col('uxid_confidence').desc_nulls_last(),
            col('batch_record_id').asc_nulls_last()
        )

        email_map = (
            df_batch.filter(col('email').isNotNull() & col('simple_uxid').isNotNull())
                    .withColumn('row_num', row_number().over(window_email))
                    .filter(col('row_num') == 1)
                    .select('email', col('simple_uxid').alias('email_uxid'))
                    .distinct()
        )

        df_batch = df_batch.join(email_map, 'email', 'left')

        df_batch = df_batch.withColumn(
            'simple_uxid',
            when(col('simple_uxid').isNull() & col('email_uxid').isNotNull(), col('email_uxid'))
            .otherwise(col('simple_uxid'))
        )

        # Use graph connected components algorithm to determine which simple_uxid values should be combined together.
        # Create a graph where vertices are defined by distinct values of simple_uxid column, and edges are defined such that
        # distinct values of simple_uxid are connected by an edge if the same guest_id value is associated with these distinct simple_uxid values.
        # Then run connected components operation to label the vertices into components.

        # Calculate df_data_for_graph_edges, which has 2 non-null, distinct columns rules_guest_id and simple_uxid.
        condition_data_for_graph = col('rules_guest_id').isNotNull() & col('simple_uxid').isNotNull()
        df_data_for_graph_edges = df_batch.select('rules_guest_id', 'simple_uxid').filter(condition_data_for_graph).distinct().orderBy('rules_guest_id')

        # Calculate df_edges whiwch has src and dst columns, describing linkage between verticies (simple_uxid values). Linkage occurs if 2 simple_uxid
        # values are associated with the same guest_id
        df_edges = df_data_for_graph_edges.withColumn('dst',
                                lag('simple_uxid').over(Window.partitionBy('rules_guest_id').orderBy('rules_guest_id'))
        ).filter(col('dst').isNotNull())
        df_edges = df_edges.withColumnRenamed("simple_uxid", "src")
        df_edges = df_edges.select('src', 'dst').distinct()

        print('Calculate edges for guest_id rules graph...')
        df_edges = df_edges.persist(StorageLevel.DISK_ONLY)
        df_edges.count()
        print('Calculate edges for guest_id rules graph...Done')
        print('')

        # The vertices are simple_uxid values, which should be non-null and distinct
        df_vertices = df_batch.select('simple_uxid').filter(col('simple_uxid').isNotNull()).distinct()
        df_vertices = df_vertices.withColumn('id', col('simple_uxid'))

        print('Calculate edges for guest_id rules graph...')
        df_vertices = df_vertices.persist(StorageLevel.DISK_ONLY)
        df_vertices.count()
        print('Calculate edges for guest_id rules graph...Done')
        print('')

        # #writing to S3
        # id_res_id: str = datetime.now().strftime("%Y%m%d%H%M%S")
        # df_path1 = f's3://udx-cust-mesh-pp-temp-results/df_edges/{id_res_id}/'
        # df_path2 = f's3://udx-cust-mesh-pp-temp-results/df_vertices/{id_res_id}/'
        # df_edges.write.mode('overwrite').parquet(df_path1)
        # df_vertices.write.mode('overwrite').parquet(df_path2)

        print('Calculate guest_id rules graph connected components...')
        gf = GraphFrame(df_vertices, df_edges)
        df_guest_id_graph_comp = gf.connectedComponents()
        df_guest_id_graph_comp = df_guest_id_graph_comp.drop('id').persist(StorageLevel.DISK_ONLY)
        df_guest_id_graph_comp.count()
        print('Calculate guest_id rules graph connected components...Done')
        print('')

        df_edges.unpersist()
        df_vertices.unpersist()

        df_batch = df_batch.join(df_guest_id_graph_comp, 'simple_uxid', 'left')

        #########################
        # Sanity checking that the same simple_uxid is not in different guest_id graph components
        print('Find records with the same simple_uxid is in multiple guest_id graph components. Should be 0 records...')
        filtered_df = df_batch.alias("a").join(
            df_batch.alias("b"),
            (col("a.component") != col("b.component")) & (col("a.simple_uxid") == col("b.simple_uxid"))
        ).select("a.*").distinct()

        if filtered_df.count() != 0:
            error_msg: str = 'ERRORERROR: The same simple_uxid has appeared in multiple guest_id graph components. This is not supposed to be possible, so throwing error rather than continuing.'
            print(error_msg)
            #filtered_df.filter('uxid is null').select('source_name', 'source_customer_id', 'rules_guest_id', 'MatchID','uxid', 'uxid_confidence', 'id_res_last_updated_timestamp', 'simple_uxid', 'component').orderBy('component', 'rules_guest_id').show(truncate=False)
            raise ValueError(error_msg)

        print('Find records with the same simple_uxid is in multiple guest_id graph components. Should be 0 records...Done')
        print('')
        #########################

        df_batch_component_null = df_batch.filter('component is null')
        df_batch_component_not_null = df_batch.filter('component is not null')

        # Create a window function that will be used to get the uxid value for each connected component that has the lowest uxid_confidence, earliest uxid creation timestamp.
        # If still ambiguous, then sort by unique batch_record_id field which gaurantees the window function will be deterministic
        window_spec = Window.partitionBy("component").orderBy(col("id_res_last_updated_timestamp").asc_nulls_last(), col("uxid_confidence").cast(DoubleType()).desc_nulls_last(), col("batch_record_id").asc_nulls_last())

        print('Use connected components to decide uxid...')
        df_batch_component_not_null_results = df_batch_component_not_null.withColumn(
            "uxid",
            first('simple_uxid', ignorenulls=True).over(window_spec)
        ).withColumn(
            "uxid_confidence",
            first('uxid_confidence', ignorenulls=True).over(window_spec)
        ).withColumn(
            "id_res_last_updated_timestamp",
            first('id_res_last_updated_timestamp', ignorenulls=True).over(window_spec)
        )

        # Sanity checking that uxid, uxid_confidence, and id_res_last_updated_timestamp are all present
        if df_batch_component_not_null_results.filter('uxid is null').count() != 0:
            error_msg: str = 'ERRORERROR: While calculating uxid for df_batch_component_not_null, connected components calculated uxid was found to be null. This is not supposed to be possible, so throwing error rather than continuing.'
            print(error_msg)
            #df_batch_component_not_null_results.filter('uxid is null').select('source_name', 'source_customer_id', 'rules_guest_id', 'MatchID','uxid', 'uxid_confidence', 'id_res_last_updated_timestamp', 'simple_uxid', 'component').orderBy('component', 'rules_guest_id').show(truncate=False)
            raise ValueError(error_msg)
        elif df_batch_component_not_null_results.filter('uxid_confidence is null').count() != 0:
            error_msg: str = 'ERRORERROR: While calculating uxid_confidence for df_batch_component_not_null, connected components calculated uxid_confidence was found to be null. This is not supposed to be possible, so throwing error rather than continuing.'
            print(error_msg)
            #df_batch_component_not_null_results.filter('uxid_confidence is null').select('source_name', 'source_customer_id', 'rules_guest_id', 'MatchID','uxid', 'uxid_confidence', 'id_res_last_updated_timestamp', 'simple_uxid', 'component').orderBy('component', 'rules_guest_id').show(truncate=False)
            raise ValueError(error_msg)
        elif df_batch_component_not_null_results.filter('id_res_last_updated_timestamp is null').count() != 0:
            error_msg: str = 'ERRORERROR: While calculating id_res_last_updated_timestamp for df_batch_component_not_null, connected components calculated id_res_last_updated_timestamp was found to be null. This is not supposed to be possible, so throwing error rather than continuing.'
            print(error_msg)
            #df_batch_component_not_null_results.filter('id_res_last_updated_timestamp is null').select('source_name', 'source_customer_id', 'rules_guest_id', 'MatchID','uxid', 'uxid_confidence', 'id_res_last_updated_timestamp', 'simple_uxid', 'component').orderBy('component', 'rules_guest_id').show(truncate=False)
            raise ValueError(error_msg)
        print('Use connected components to decide uxid...Done')
        print('')
        
        print('Combine results dataframes...')
        df_batch_results = df_batch_component_not_null_results.unionByName(df_batch_component_null)

        # Clean up temporary columns
        df_batch_results = df_batch_results.drop('new_uxid', 'component', 'simple_uxid', 'email_uxid')
        # tmp_mapping_fields = [c for c in df_batch.columns if c.startswith('temp_mapping_')]
        # df_batch = df_batch.drop(*tmp_mapping_fields)

        # Checkpointing df_batch to ensure that the triggered recalculations use this point in the code as a starting point instead of going back
        df_batch = df_batch.checkpoint(eager=True)
        print('Combine results dataframes...Done')
        print('')

        return df_batch_results
