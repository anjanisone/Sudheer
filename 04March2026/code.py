import sys
import uuid
import json
import boto3
import pandas as pd
from datetime import datetime
from io import BytesIO

from awsglue.utils import getResolvedOptions
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


# ---------------------------------------------------
# LOAD GLUE JOB PARAMETERS
# ---------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        'temp_s3_bucket',
        'output_s3_bucket',
        'ES_HOST',
        'ES_REGION',
        'ES_IAM_MASTER_USER_ARN'
    ]
)


# ---------------------------------------------------
# CONFIG CLASS
# ---------------------------------------------------

class Config:

    INPUT_PREFIX = "opensearch_cleanup_inputs/Unprocessed/"
    PROCESSED_PREFIX = "opensearch_cleanup_inputs/Processed/"

    BACKUP_PREFIX = "opensearch_cleanup_backups/"
    PARQUET_PREFIX = "opensearch_cleanup_outputs/"

    INDEX_NAME = "udx_identity"

    DYNAMODB_TABLE = "udx-cust-mesh-p-neptune-status-table-prod"

    REGION = args['ES_REGION']
    OPENSEARCH_HOST = args['ES_HOST']

    TEMP_BUCKET = args['temp_s3_bucket']
    OUTPUT_BUCKET = args['output_s3_bucket']


# ---------------------------------------------------
# S3 MANAGER
# ---------------------------------------------------

class S3Manager:

    def __init__(self):
        self.s3 = boto3.client("s3")

    def get_unprocessed_file(self):

        response = self.s3.list_objects_v2(
            Bucket=Config.TEMP_BUCKET,
            Prefix=Config.INPUT_PREFIX
        )

        for obj in response.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                return key

        raise Exception("No CSV found in Unprocessed folder")

    def read_csv(self, key):

        obj = self.s3.get_object(
            Bucket=Config.TEMP_BUCKET,
            Key=key
        )

        return pd.read_csv(obj["Body"])

    def backup_document(self, doc_id, doc):

        key = f"{Config.BACKUP_PREFIX}{doc_id}.json"

        self.s3.put_object(
            Bucket=Config.TEMP_BUCKET,
            Key=key,
            Body=json.dumps(doc).encode()
        )

    def save_parquet(self, df, job_id):

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)

        key = f"{Config.PARQUET_PREFIX}{job_id}/remediated.parquet"

        self.s3.put_object(
            Bucket=Config.OUTPUT_BUCKET,
            Key=key,
            Body=buffer.getvalue()
        )

        return f"s3://{Config.OUTPUT_BUCKET}/{key}"

    def move_to_processed(self, key):

        filename = key.split("/")[-1]

        new_key = f"{Config.PROCESSED_PREFIX}{filename}"

        self.s3.copy_object(
            Bucket=Config.TEMP_BUCKET,
            CopySource={"Bucket": Config.TEMP_BUCKET, "Key": key},
            Key=new_key
        )

        self.s3.delete_object(
            Bucket=Config.TEMP_BUCKET,
            Key=key
        )


# ---------------------------------------------------
# OPENSEARCH MANAGER
# ---------------------------------------------------

class OpenSearchManager:

    def __init__(self):

        session = boto3.Session()
        credentials = session.get_credentials()

        auth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            Config.REGION,
            "es",
            session_token=credentials.token
        )

        self.client = OpenSearch(
            hosts=[{"host": Config.OPENSEARCH_HOST, "port": 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

    def fetch_record(self, doc_id):

        try:
            return self.client.get(
                index=Config.INDEX_NAME,
                id=doc_id
            )
        except:
            return None

    def delete_record(self, doc_id):

        self.client.delete(
            index=Config.INDEX_NAME,
            id=doc_id
        )

    def bulk_insert(self, records):

        body = []

        for r in records:

            body.append({
                "index": {
                    "_index": Config.INDEX_NAME,
                    "_id": r["_id"]
                }
            })

            body.append(r["_source"])

        self.client.bulk(body)


# ---------------------------------------------------
# IDENTITY PROCESSOR
# ---------------------------------------------------

class IdentityProcessor:

    def regenerate(self, source):

        now = datetime.utcnow().isoformat()

        old_uxid = source.get("uxid")

        new_uxid = str(uuid.uuid4())

        past_uxids = source.get("past_uxids", [])

        if old_uxid:

            past_uxids.append({
                "uxid": old_uxid,
                "uxid_confidence": source.get("uxid_confidence", 1.0),
                "uxid_last_changed_timestamp": now
            })

        source["uxid"] = new_uxid

        source["uxid_confidence"] = 1.0

        source["past_uxids"] = past_uxids

        source["created_timestamp"] = now

        source["last_updated_timestamp"] = now

        return source


# ---------------------------------------------------
# DYNAMODB PIPELINE TRIGGER
# ---------------------------------------------------

class PipelineTrigger:

    def __init__(self):
        self.dynamo = boto3.client("dynamodb")

    def trigger(self, job_id, count, path):

        self.dynamo.put_item(
            TableName=Config.DYNAMODB_TABLE,
            Item={
                "IdResJobId": {"S": job_id},
                "Status": {"S": "ready-for-neptune-load"},
                "RecordCount": {"N": str(count)},
                "DataPath": {"S": path},
                "EventTime": {"S": datetime.utcnow().strftime("%Y%m%d%H%M%S")}
            }
        )


# ---------------------------------------------------
# MAIN JOB
# ---------------------------------------------------

class OpenSearchCleanupJob:

    def __init__(self):

        self.s3 = S3Manager()
        self.os = OpenSearchManager()
        self.identity = IdentityProcessor()
        self.pipeline = PipelineTrigger()

        self.job_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    def run(self):

        csv_key = self.s3.get_unprocessed_file()

        df = self.s3.read_csv(csv_key)

        records = []
        parquet_rows = []

        for _, row in df.iterrows():

            doc_id = row["doc_id"]

            record = self.os.fetch_record(doc_id)

            if not record:
                continue

            self.s3.backup_document(doc_id, record)

            self.os.delete_record(doc_id)

            source = record["_source"]

            new_source = self.identity.regenerate(source)

            records.append({
                "_id": doc_id,
                "_source": new_source
            })

            parquet_rows.append(new_source)

        if records:
            self.os.bulk_insert(records)

        parquet_df = pd.DataFrame(parquet_rows)

        parquet_path = self.s3.save_parquet(parquet_df, self.job_id)

        self.pipeline.trigger(
            self.job_id,
            len(parquet_rows),
            parquet_path
        )

        self.s3.move_to_processed(csv_key)


# ---------------------------------------------------
# ENTRY
# ---------------------------------------------------

if __name__ == "__main__":

    job = OpenSearchCleanupJob()

    job.run()
