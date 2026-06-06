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

