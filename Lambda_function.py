import boto3
import json
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ATHENA_DATABASE    = "healthcare_db"
ATHENA_TABLE       = "facilities"
OUTPUT_BUCKET      = "medlaunch-068410981074-us-east-1-an"          # <-- change this
OUTPUT_PREFIX      = "query-results/"
RESULTS_PREFIX     = "athena-state-counts/"
MAX_WAIT_SECONDS   = 55   # Lambda default timeout is 60s, stop polling before that
POLL_INTERVAL      = 3


def run_athena_query(athena_client, query: str, output_location: str) -> str:
    """Start Athena query and return execution ID."""
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": output_location},
    )
    query_id = response["QueryExecutionId"]
    logger.info(f"Started Athena query: {query_id}")
    return query_id


def wait_for_query(athena_client, query_id: str) -> str:
    """Poll until query finishes. Returns final state."""
    elapsed = 0
    while elapsed < MAX_WAIT_SECONDS:
        response = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = response["QueryExecution"]["Status"]["State"]
        logger.info(f"Query {query_id} state: {state} ({elapsed}s elapsed)")

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return state

        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

    logger.warning(f"Query {query_id} still running after {elapsed}s — Lambda timeout approaching")
    return "TIMEOUT"


def get_query_results(athena_client, query_id: str) -> list[dict]:
    """Fetch rows from completed Athena query."""
    response  = athena_client.get_query_results(QueryExecutionId=query_id)
    rows      = response["ResultSet"]["Rows"]
    headers   = [col["VarCharValue"] for col in rows[0]["Data"]]
    results   = []
    for row in rows[1:]:   # skip header row
        values = [col.get("VarCharValue", "") for col in row["Data"]]
        results.append(dict(zip(headers, values)))
    return results


def copy_results_to_output(s3_client, query_id: str, bucket: str, dest_prefix: str):
    """Copy Athena result CSV from query-results/ to a named output location."""
    src_key  = f"{OUTPUT_PREFIX}{query_id}.csv"
    dest_key = f"{dest_prefix}state_counts_latest.csv"
    s3_client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": src_key},
        Key=dest_key,
    )
    logger.info(f"Copied results to s3://{bucket}/{dest_key}")


def lambda_handler(event, context):
    logger.info(f"Triggered by event: {json.dumps(event)}")

    # Extract the uploaded file info from the S3 event
    try:
        record      = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        object_key  = record["s3"]["object"]["key"]
        logger.info(f"New file uploaded: s3://{bucket_name}/{object_key}")
    except (KeyError, IndexError) as e:
        logger.error(f"Could not parse S3 event: {e}")
        raise ValueError(f"Invalid S3 event structure: {e}")

    athena = boto3.client("athena", region_name="us-east-1")
    s3     = boto3.client("s3",     region_name="us-east-1")

    output_location = f"s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}"

    # Count accredited facilities per state
    query = f"""
        SELECT
            location.state           AS state,
            COUNT(*)                 AS accredited_facility_count
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE cardinality(accreditations) > 0
        GROUP BY location.state
        ORDER BY accredited_facility_count DESC
    """

    # Run query
    query_id = run_athena_query(athena, query, output_location)

    # Wait for result
    final_state = wait_for_query(athena, query_id)

    if final_state == "SUCCEEDED":
        results = get_query_results(athena, query_id)
        logger.info(f"Query results: {json.dumps(results)}")
        copy_results_to_output(s3, query_id, OUTPUT_BUCKET, RESULTS_PREFIX)
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Query succeeded",
                "query_id": query_id,
                "results": results
            })
        }
    else:
        error_msg = f"Athena query {query_id} ended with state: {final_state}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)