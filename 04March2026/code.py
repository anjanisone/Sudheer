import boto3
import uuid
import json
import pandas as pd
from datetime import datetime
from io import BytesIO
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


# ------------------------------------------------
# CONFIGURATION
# ------------------------------------------------

class Config:

    REGION = "us-east-1"

    BUCKET = "udx-cust-mesh-p-scratch-space"

    INPUT_PREFIX = "opensearch_cleanup_inputs/Unprocessed/"
    PROCESSED_PREFIX = "opensearch_cleanup_inputs/Processed/"

    BACKUP_PREFIX = "opensearch_cleanup_backups/"
    PARQUET_PREFIX = "opensearch_cleanup_outputs/"

    INDEX_NAME = "udx_identity"

    OPENSEARCH_HOST = "YOUR_OPENSEARCH_ENDPOINT"

    DYNAMODB_TABLE = "neptune_status_table"


# ------------------------------------------------
# S3 MANAGER
# ------------------------------------------------

class S3Manager:

    def __init__(self):
        self.s3 = boto3.client("s3")

    def get_unprocessed_file(self):

        response = self.s3.list_objects_v2(
            Bucket=Config.BUCKET,
            Prefix=Config.INPUT_PREFIX
        )

        if "Contents" not in response:
            raise Exception("No files in Unprocessed folder")

        for obj in response["Contents"]:
            key = obj["Key"]
            if key.endswith(".csv"):
                return key

        raise Exception("No CSV file found")

    def read_csv(self, key):

        obj = self.s3.get_object(
            Bucket=Config.BUCKET,
            Key=key
        )

        return pd.read_csv(obj["Body"])

    def backup_document(self, doc_id, document):

        key = f"{Config.BACKUP_PREFIX}{doc_id}.json"

        self.s3.put_object(
            Bucket=Config.BUCKET,
            Key=key,
            Body=json.dumps(document).encode("utf-8")
        )

    def save_parquet(self, df, job_id):

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)

        key = f"{Config.PARQUET_PREFIX}{job_id}/remediated.parquet"

        self.s3.put_object(
            Bucket=Config.BUCKET,
            Key=key,
            Body=buffer.getvalue()
        )

        return f"s3://{Config.BUCKET}/{key}"

    def move_to_processed(self, key):

        filename = key.split("/")[-1]
        new_key = f"{Config.PROCESSED_PREFIX}{filename}"

        self.s3.copy_object(
            Bucket=Config.BUCKET,
            CopySource={"Bucket": Config.BUCKET, "Key": key},
            Key=new_key
        )

        self.s3.delete_object(
            Bucket=Config.BUCKET,
            Key=key
        )


# ------------------------------------------------
# OPENSEARCH MANAGER
# ------------------------------------------------

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


# ------------------------------------------------
# IDENTITY PROCESSOR
# ------------------------------------------------

class IdentityProcessor:

    def regenerate(self, source):

        new_uxid = str(uuid.uuid4())

        old_uxid = source.get("uxid")

        past = source.get("past_uxids", [])

        if old_uxid:
            past.append({
                "uxid": old_uxid,
                "uxid_last_changed_timestamp": datetime.utcnow().isoformat()
            })

        source["uxid"] = new_uxid
        source["past_uxids"] = past

        return source


# ------------------------------------------------
# PIPELINE TRIGGER
# ------------------------------------------------

class PipelineTrigger:

    def __init__(self):
        self.dynamo = boto3.client("dynamodb")

    def trigger(self, job_id, record_count, data_path):

        self.dynamo.put_item(
            TableName=Config.DYNAMODB_TABLE,
            Item={
                "IdResJobId": {"S": job_id},
                "Status": {"S": "ready-for-neptune-load"},
                "RecordCount": {"N": str(record_count)},
                "DataPath": {"S": data_path},
                "EventTime": {"S": datetime.utcnow().strftime("%Y%m%d%H%M%S")}
            }
        )


# ------------------------------------------------
# MAIN JOB
# ------------------------------------------------

class OpenSearchCleanupJob:

    def __init__(self):

        self.s3 = S3Manager()
        self.os = OpenSearchManager()
        self.identity = IdentityProcessor()
        self.pipeline = PipelineTrigger()

        self.job_id = datetime.utcnow().strftime("%Y%m%d%H%M%S") + "_adhoc_remediation"

    def process(self):

        csv_key = self.s3.get_unprocessed_file()

        df = self.s3.read_csv(csv_key)

        records_to_insert = []
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

            records_to_insert.append({
                "_id": doc_id,
                "_source": new_source
            })

            parquet_rows.append(new_source)

        if records_to_insert:
            self.os.bulk_insert(records_to_insert)

        parquet_df = pd.DataFrame(parquet_rows)

        parquet_path = self.s3.save_parquet(parquet_df, self.job_id)

        self.pipeline.trigger(
            self.job_id,
            len(parquet_rows),
            parquet_path
        )

        self.s3.move_to_processed(csv_key)


# ------------------------------------------------
# ENTRY POINT
# ------------------------------------------------

if __name__ == "__main__":

    job = OpenSearchCleanupJob()

    job.process()
