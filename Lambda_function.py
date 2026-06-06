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

