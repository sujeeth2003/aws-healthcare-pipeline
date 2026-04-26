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

    logger.info(f"Loaded {len(facilities)} facility records")
    return facilities


def has_expiring_accreditation(facility, cutoff_date):
    accreditations = facility.get("accreditations", [])
    if not accreditations:
        return False

    for acc in accreditations:
        valid_until_str = acc.get("valid_until", "")
        if not valid_until_str:
            continue
        try:
            valid_until = datetime.strptime(valid_until_str, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            if valid_until <= cutoff_date:
                return True
        except ValueError as e:
            logger.warning(
                f"Could not parse date '{valid_until_str}' for "
                f"facility {facility.get('facility_id', 'UNKNOWN')}: {e}"
            )

    return False


def write_facilities_to_s3(s3_client, bucket, key, facilities):
    logger.info(f"Writing {len(facilities)} records to s3://{bucket}/{key}")
    try:
        ndjson_content = "\n".join(json.dumps(f) for f in facilities)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=ndjson_content.encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("Upload successful")
    except Exception as e:
        logger.error(f"Failed to write to S3: {e}")
        raise


def main():
    now    = datetime.now(tz=timezone.utc)
    cutoff = now + relativedelta(months=MONTHS_AHEAD)
    logger.info(f"Today: {now.date()} | Cutoff (6 months out): {cutoff.date()}")

    s3 = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    facilities = read_facilities_from_s3(s3, BUCKET_NAME, INPUT_PREFIX)

    expiring = [f for f in facilities if has_expiring_accreditation(f, cutoff)]
    logger.info(
        f"Facilities with accreditations expiring within 6 months: "
        f"{len(expiring)} / {len(facilities)}"
    )

    for f in expiring:
        logger.info(f"  → {f['facility_id']} — {f['facility_name']}")

    if expiring:
        write_facilities_to_s3(s3, BUCKET_NAME, OUTPUT_PREFIX, expiring)
    else:
        logger.info("No expiring facilities found. Nothing written to output.")

    logger.info("Done.")


if __name__ == "__main__":
    main()