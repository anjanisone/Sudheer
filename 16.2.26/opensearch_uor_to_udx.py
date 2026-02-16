from opensearchpy import OpenSearch, helpers

HOST = "https://your-opensearch-endpoint"
USERNAME = "admin"
PASSWORD = "password"
INDEX = "idstore_1"

OLD_PREFIX = "uor_fnbo"
NEW_PREFIX = "udx_fnbo"

client = OpenSearch(
    HOST,
    http_auth=(USERNAME, PASSWORD),
    use_ssl=True,
    verify_certs=False
)

query = {
    "query": {
        "prefix": {
            "_id": OLD_PREFIX
        }
    }
}

response = client.search(index=INDEX, body=query, scroll="2m", size=1000)
scroll_id = response['_scroll_id']
hits = response['hits']['hits']

while hits:
    actions = []
    for doc in hits:
        old_id = doc["_id"]
        new_id = old_id.replace(OLD_PREFIX, NEW_PREFIX)
        source = doc["_source"]

        if "source_name" in source and source["source_name"]:
            source["source_name"] = source["source_name"].replace(OLD_PREFIX, NEW_PREFIX)

        actions.append({
            "_op_type": "index",
            "_index": INDEX,
            "_id": new_id,
            "_source": source
        })

        actions.append({
            "_op_type": "delete",
            "_index": INDEX,
            "_id": old_id
        })

    helpers.bulk(client, actions)
    response = client.scroll(scroll_id=scroll_id, scroll="2m")
    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']

print("Done")
