# ================================
# OpenSearch PII Cleanup – Glue Notebook
# Operations: UPDATE / NULLIFY / DELETE FIELD
# Using boto3 + requests + AWS4Auth (no opensearch-py)
# ================================

import boto3
import json
import math
import re
from datetime import datetime

import requests
from requests_aws4auth import AWS4Auth

# ========= CONFIG =========

ES_HOST = "vpc-udx-cust-mesh-p-prod-62s3jnxbzj6oga4jbnxf37yba4.us-east-1.es.amazonaws.com"
ES_REGION = "us-east-1"
ES_IAM_MASTER_USER_ARN = "arn:aws:iam::619071330720:role/udx-cust-mesh-p-opensearch-master-user-role-prod"

INDEX_NAME = "idstore_1"

ES_BATCH_SIZE = 500       # docs per scroll page
DRY_RUN = True            # <<< SET TO False TO ACTUALLY WRITE >>>

# OPTIONS: "NULLIFY", "UPDATE", "DELETE_FIELD"
OPERATION = "NULLIFY"

FIELD_NAME = "record_normalized.ml_email_address"

VALUES_TO_PROCESS = [
    "avonniessen@hotmail.com",
    "ask@travelmagic.com",
    "info@mimotrips.com",
]

UPDATE_MAPPING = {
    # "old_email@example.com": "new_email@example.com"
}

# ========= AUTH / CLIENT SETUP =========

def get_awsauth_and_endpoint():
    print(f"[{datetime.utcnow()}] Assuming role: {ES_IAM_MASTER_USER_ARN}")
    sts_client = boto3.client("sts")
    assumed_role = sts_client.assume_role(
        RoleArn=ES_IAM_MASTER_USER_ARN,
        RoleSessionName="GlueOpenSearchCleanupSession",
    )
    credentials = assumed_role["Credentials"]

    awsauth = AWS4Auth(
        credentials["AccessKeyId"],
        credentials["SecretAccessKey"],
        ES_REGION,
        "es",
        session_token=credentials["SessionToken"],
    )

    endpoint = f"https://{ES_HOST}"
    return awsauth, endpoint


AWS_AUTH, ES_ENDPOINT = get_awsauth_and_endpoint()

def os_json_request(method, path, body=None, params=None):
    url = ES_ENDPOINT + path
    headers = {"Content-Type": "application/json"}

    data = json.dumps(body) if body is not None else None

    resp = requests.request(
        method=method,
        url=url,
        auth=AWS_AUTH,
        headers=headers,
        params=params,
        data=data,
        timeout=60,
    )

    if not resp.ok:
        raise RuntimeError(
            f"OpenSearch request failed: {resp.status_code} {resp.text}"
        )

    return resp.json()


def os_bulk_request(ndjson_body):
    url = ES_ENDPOINT + "/_bulk"
    headers = {"Content-Type": "application/x-ndjson"}

    resp = requests.post(
        url=url,
        auth=AWS_AUTH,
        headers=headers,
        data=ndjson_body,
        timeout=300,
    )

    if not resp.ok:
        raise RuntimeError(
            f"OpenSearch bulk request failed: {resp.status_code} {resp.text}"
        )

    return resp.json()

# ========= SEARCH HELPER (SCROLL) =========

def search_docs_by_field(index, field_name, field_value, batch_size=ES_BATCH_SIZE):
    print(f"\n Searching for {field_name} = {field_value}")

    query = {
        "query": {
            "term": {
                field_name: {
                    "value": field_value
                }
            }
        }
    }

    first_page = os_json_request(
        method="POST",
        path=f"/{index}/_search",
        body=query,
        params={"scroll": "2m", "size": batch_size},
    )

    scroll_id = first_page.get("_scroll_id")
    hits = first_page["hits"]["hits"]
    total_hits_obj = first_page["hits"]["total"]
    total_hits = (
        total_hits_obj["value"]
        if isinstance(total_hits_obj, dict)
        else total_hits_obj
    )

    print(f" Found {total_hits} hits for value: {field_value}")

    for h in hits:
        yield h

    while True:
        if not hits or not scroll_id:
            break

        scroll_page = os_json_request(
            method="POST",
            path="/_search/scroll",
            body={"scroll": "2m", "scroll_id": scroll_id},
        )
        scroll_id = scroll_page.get("_scroll_id")
        hits = scroll_page["hits"]["hits"]

        if not hits:
            break

        for h in hits:
            yield h

# ========= BULK OPERATIONS (REST) =========

def bulk_nullify_field(index, field_name, field_value, dry_run=True):
    actions = []

    for hit in search_docs_by_field(index, field_name, field_value):
        doc_id = hit["_id"]

        actions.append(json.dumps({
            "update": {
                "_index": index,
                "_id": doc_id
            }
        }))
        actions.append(json.dumps({
            "doc": {
                field_name: None
            }
        }))

    if not actions:
        print(f" No documents to nullify for {field_name} = {field_value}")
        return

    print(f" Prepared {len(actions) // 2} nullify actions for {field_name} = {field_value}")

    if dry_run:
        print("[DRY_RUN] Not sending bulk nullify request.")
        return

    ndjson_body = "\n".join(actions) + "\n"
    resp = os_bulk_request(ndjson_body)

    items = resp.get("items", [])
    success = 0
    for item in items:
        upd = item.get("update", {})
        status = upd.get("status", 0)
        if 200 <= status < 300:
            success += 1
    failed = len(items) - success

    print(f" Bulk nullify completed: success={success}, failed={failed}")


def bulk_update_field(index, field_name, old_value, new_value, dry_run=True):
    actions = []

    for hit in search_docs_by_field(index, field_name, old_value):
        doc_id = hit["_id"]

        actions.append(json.dumps({
            "update": {
                "_index": index,
                "_id": doc_id
            }
        }))
        actions.append(json.dumps({
            "doc": {
                field_name: new_value
            }
        }))

    if not actions:
        print(f" No documents to update for {field_name} = {old_value}")
        return

    print(f" Prepared {len(actions) // 2} update actions for {field_name}: {old_value} → {new_value}")

    if dry_run:
        print("[DRY_RUN] Not sending bulk update request.")
        return

    ndjson_body = "\n".join(actions) + "\n"
    resp = os_bulk_request(ndjson_body)

    items = resp.get("items", [])
    success = 0
    for item in items:
        upd = item.get("update", {})
        status = upd.get("status", 0)
        if 200 <= status < 300:
            success += 1
    failed = len(items) - success

    print(f" Bulk update completed: success={success}, failed={failed}")


def bulk_delete_field(index, field_name, field_value, dry_run=True):
    actions = []

    for hit in search_docs_by_field(index, field_name, field_value):
        doc_id = hit["_id"]

        actions.append(json.dumps({
            "update": {
                "_index": index,
                "_id": doc_id
            }
        }))
        actions.append(json.dumps({
            "script": {
                "source": f"ctx._source.remove('{field_name}')",
                "lang": "painless"
            }
        }))

    if not actions:
        print(f" No documents to delete field {field_name} for value {field_value}")
        return

    print(f" Prepared {len(actions) // 2} delete-field actions for {field_name} = {field_value}")

    if dry_run:
        print("[DRY_RUN] Not sending bulk delete-field request.")
        return

    ndjson_body = "\n".join(actions) + "\n"
    resp = os_bulk_request(ndjson_body)

    items = resp.get("items", [])
    success = 0
    for item in items:
        upd = item.get("update", {})
        status = upd.get("status", 0)
        if 200 <= status < 300:
            success += 1
    failed = len(items) - success

    print(f" Bulk delete-field completed: success={success}, failed={failed}")

# ========= DRIVER / MAIN EXECUTION =========

print(f"\n Starting operation: {OPERATION}")
print(f"Target index: {INDEX_NAME}, field: {FIELD_NAME}, DRY_RUN={DRY_RUN}")

if OPERATION == "NULLIFY":
    for val in VALUES_TO_PROCESS:
        bulk_nullify_field(
            index=INDEX_NAME,
            field_name=FIELD_NAME,
            field_value=val,
            dry_run=DRY_RUN,
        )

elif OPERATION == "DELETE_FIELD":
    for val in VALUES_TO_PROCESS:
        bulk_delete_field(
            index=INDEX_NAME,
            field_name=FIELD_NAME,
            field_value=val,
            dry_run=DRY_RUN,
        )

elif OPERATION == "UPDATE":
    for old_val, new_val in UPDATE_MAPPING.items():
        bulk_update_field(
            index=INDEX_NAME,
            field_name=FIELD_NAME,
            old_value=old_val,
            new_value=new_val,
            dry_run=DRY_RUN,
        )
else:
    print(f"Unsupported OPERATION: {OPERATION}. Choose from: 'NULLIFY', 'UPDATE', 'DELETE_FIELD'.")

print("\n Done.")
