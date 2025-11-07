import boto3
import time

NEPTUNE_CLUSTER_ENDPOINT = ""
REGION = "us-east-1"
LABEL = "Email"
EMAIL_FIELD = "email_address"
DRY_RUN = True
BATCH_SIZE = 50

emails_to_nullify = [
    ""
]

neptune_client = boto3.client(
    "neptunedata",
    region_name=REGION,
    endpoint_url=f"{NEPTUNE_CLUSTER_ENDPOINT}:8182"
)

updated_count = 0

for email_val in emails_to_nullify:
    print(f"\nProcessing email: {email_val}")

    query_find = f"""
    MATCH (n:{LABEL})
    WHERE n.{EMAIL_FIELD} = '{email_val.lower()}'
    RETURN n.id AS id
    """

    try:
        res = neptune_client.execute_open_cypher_query(openCypherQuery=query_find)
        results = res.get("results", [])
        if not results:
            print("No vertices found for this email.")
            continue

        ids = [r.get("id") for r in results if r.get("id")]
        print(f"Vertices to update: {len(ids)}")

        if not DRY_RUN:
            for i, vid in enumerate(ids):
                q = f"""
                MATCH (n:{LABEL} {{id:'{vid}'}})
                REMOVE n.{EMAIL_FIELD}
                """
                try:
                    neptune_client.execute_open_cypher_query(openCypherQuery=q)
                    updated_count += 1
                    print(f"Email nullified for vertex: {vid}")
                except Exception as e:
                    print(f"Failed to nullify vertex {vid}: {e}")
                if (i + 1) % BATCH_SIZE == 0:
                    time.sleep(0.2)
        else:
            print(f"[Dry Run] Would nullify {len(ids)} vertices for email: {email_val}")

    except Exception as ex:
        print(f"Error processing {email_val}: {ex}")

print("\n=== Cleanup Summary ===")
print(f"Emails processed: {len(emails_to_nullify)}")
print(f"Vertices updated: {updated_count}")
print(f"Dry run: {DRY_RUN}")
print("========================")
