import boto3
import json
from datetime import datetime
import requests
from requests_aws4auth import AWS4Auth

ES_HOST = "vpc-udx-cust-mesh-p-prod-62s3jnxbzj6oga4jbnxf37yba4.us-east-1.es.amazonaws.com"
ES_REGION = "us-east-1"
ES_IAM_MASTER_USER_ARN = "arn:aws:iam::619071330720:role/udx-cust-mesh-p-opensearch-master-user-role-prod"

INDEX_NAME = "idstore_1"
ES_BATCH_SIZE = 500
DRY_RUN = True

OPERATION = "NULLIFY"  # for this rule

FIELD_NAME = "record_normalized.ml_email_address"

VALUES_TO_PROCESS = [
    "avonniessen@hotmail.com",
    "ask@travelmagic.com",
    "info@mimotrips.com",
]

# fields used to compute "null-count" per item
PII_FIELDS = [
    "record_normalized.ml_email_address",
    "record_normalized.address_line_1",
    "record_normalized.address_line_2",
    "record_normalized.gender",
]

NULL_THRESHOLD = 3  # >3 ⇒ delete doc, else nullify FIELD_NAME


def get_awsauth_and_endpoint():
    sts_client = boto3.client("sts")
    assumed_role = sts_client.assume_role(
        RoleArn=ES_IAM_MASTER_USER_ARN,
        RoleSessionName="GlueOpenSearchCleanupSession",
    )
    c = assumed_role["Credentials"]
    awsauth = AWS4Auth(
        c["AccessKeyId"],
        c["SecretAccessKey"],
        ES_REGION,
        "es",
        session_token=c["SessionToken"],
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
        raise RuntimeError(f"OpenSearch request failed: {resp.status_code} {resp.text}")
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
        raise RuntimeError(f"OpenSearch bulk request failed: {resp.status_code} {resp.text}")
    return resp.json()


def search_docs_by_field(index, field_name, field_value, batch_size=ES_BATCH_SIZE):
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
    total_hits = total_hits_obj["value"] if isinstance(total_hits_obj, dict) else total_hits_obj
    print(f"Found {total_hits} hits for {field_name} = {field_value}")

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


def bulk_nullify_or_delete_by_null_threshold(
    index,
    field_name,
    field_value,
    pii_fields,
    null_threshold,
    dry_run=True,
):
    actions = []
    nullify_count = 0
    delete_count = 0

    for hit in search_docs_by_field(index, field_name, field_value):
        doc_id = hit["_id"]
        src = hit.get("_source", {})

        null_count = 0
        for f in pii_fields:
            v = src
            for part in f.split("."):
                if not isinstance(v, dict) or part not in v:
                    v = None
                    break
                v = v[part]
            if v is None or (isinstance(v, str) and v.strip() == ""):
                null_count += 1

        if null_count > null_threshold:
            actions.append(json.dumps({
                "delete": {
                    "_index": index,
                    "_id": doc_id
                }
            }))
            delete_count += 1
        else:
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
            nullify_count += 1

    if not actions:
        print(f"No documents found for {field_name} = {field_value}")
        return

    print(
        f"Prepared {nullify_count} nullify actions and {delete_count} delete actions "
        f"for {field_name} = {field_value}"
    )

    if dry_run:
        print("[DRY_RUN] Not sending bulk request.")
        return

    ndjson_body = "\n".join(actions) + "\n"
    resp = os_bulk_request(ndjson_body)
    items = resp.get("items", [])
    ok = sum(1 for it in items for op in it.values() if 200 <= op.get("status", 0) < 300)
    print(f"Bulk completed; successful operations: {ok}, total meta records: {len(items)}")


print(f"[{datetime.utcnow()}] Starting operation {OPERATION}, DRY_RUN={DRY_RUN}")
print(f"Index={INDEX_NAME}, field={FIELD_NAME}, values={VALUES_TO_PROCESS}")

if OPERATION == "NULLIFY":
    for val in VALUES_TO_PROCESS:
        bulk_nullify_or_delete_by_null_threshold(
            index=INDEX_NAME,
            field_name=FIELD_NAME,
            field_value=val,
            pii_fields=PII_FIELDS,
            null_threshold=NULL_THRESHOLD,
            dry_run=DRY_RUN,
        )
else:
    print("Only NULLIFY operation is wired to the null-threshold rule in this script.")

print("Done.")
