import boto3
import json
import logging
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import os
from dotenv import load_dotenv

# ── CHANGE THESE 3 VALUES ────────────────────────────────────────────────────
BUCKET_NAME       = os.getenv('BUCKET_NAME')
AWS_ACCESS_KEY    = os.getenv('Access_key')
AWS_SECRET_KEY    = os.getenv('Secret_key')
# ─────────────────────────────────────────────────────────────────────────────

INPUT_PREFIX  = "raw-data/facilities.json"
OUTPUT_PREFIX = "filtered-output/expiring_facilities.json"
MONTHS_AHEAD  = 6

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
logger = logging.getLogger(__name__)


def read_facilities_from_s3(s3_client, bucket, key):
    logger.info(f"Reading s3://{bucket}/{key}")
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content  = response["Body"].read().decode("utf-8")
    except Exception as e:
        logger.error(f"Failed to read from S3: {e}")
        raise

    facilities = []
    for line_num, line in enumerate(content.strip().split("\n"), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            facilities.append(json.loads(line))
        except json.JSONDecodeError as e:
            logger.warning(f"Skipping malformed JSON on line {line_num}: {e}")

    main()