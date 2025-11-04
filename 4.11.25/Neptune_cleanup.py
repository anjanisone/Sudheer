import boto3
import pandas as pd
from datetime import datetime
import time
import ast

NEPTUNE_CLUSTER_ENDPOINT = ""
REGION = "us-east-1"
LABEL = "Person"
EMAIL_FIELD = "email_address"
TIMESTAMP_FIELD = "lastModified"
DRY_RUN = True
BATCH_SIZE = 100

emails_to_check = [
    "user1@company.com",
    "user2@company.com"
]

neptune_client = boto3.client("neptunedata", region_name=REGION)
records = []

for e in emails_to_check:
    query = f"""
    MATCH (n:{LABEL})
    WHERE '{e.lower()}' IN n.{EMAIL_FIELD}
       OR n.{EMAIL_FIELD} = '{e.lower()}'
    RETURN n.id AS id, n.{EMAIL_FIELD} AS email_address, n.{TIMESTAMP_FIELD} AS lastModified
    """
    try:
        res = neptune_client.execute_open_cypher_query(openCypherQuery=query)
        results = res.get("results", [])
        for r in results:
            records.append({
                "~id": r.get("id"),
                "email_address": r.get("email_address"),
                "lastModified": r.get("lastModified")
            })
    except Exception as ex:
        print(f"Error fetching {e}: {ex}")

if not records:
    print("No matching records found.")
    exit()

df = pd.DataFrame(records)
df = df.dropna(subset=["email_address"])

def parse_emails(e):
    if pd.isna(e):
        return []
    if isinstance(e, list):
        return [str(x).strip().lower() for x in e if pd.notna(x)]
    if isinstance(e, str):
        try:
            parsed = ast.literal_eval(e)
            if isinstance(parsed, list):
                return [str(x).strip().lower() for x in parsed if pd.notna(x)]
        except Exception:
            return [e.strip().lower()]
    return [str(e).strip().lower()]

df["emails_list"] = df["email_address"].apply(parse_emails)
df = df.explode("emails_list").dropna(subset=["emails_list"])
df = df.rename(columns={"emails_list": "normalized_email"})

dupes = df[df.duplicated("normalized_email", keep=False)]
if dupes.empty:
    print("No duplicates found for given emails.")
    exit()

dupes_sorted = dupes.sort_values("lastModified", ascending=False)
keepers = dupes_sorted.drop_duplicates("normalized_email", keep='first')
to_nullify = dupes_sorted[~dupes_sorted["~id"].isin(keepers["~id"])]

print(f"Duplicates found: {len(dupes)} | To nullify: {len(to_nullify)}")

if not DRY_RUN:
    for i, row in enumerate(to_nullify.itertuples(index=False)):
        vertex_id = getattr(row, "_1")
        try:
            query = f"""
            MATCH (n:{LABEL} {{id:'{vertex_id}'}})
            REMOVE n.{EMAIL_FIELD}
            """
            neptune_client.execute_open_cypher_query(openCypherQuery=query)
            print(f"Nullified email for vertex: {vertex_id}")
        except Exception as e:
            print(f"Failed to nullify vertex {vertex_id}: {e}")
        if (i + 1) % BATCH_SIZE == 0:
            time.sleep(0.3)
    print("Cleanup completed.")
else:
    print("Dry run mode — no updates applied.")

print("\n=== Cleanup Summary ===")
print(f"Total checked emails: {len(emails_to_check)}")
print(f"Duplicates found: {len(dupes)}")
print(f"Nullified vertices: {len(to_nullify)}")
print(f"Dry run: {DRY_RUN}")
print("=======================")
