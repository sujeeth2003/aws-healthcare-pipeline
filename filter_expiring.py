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


    main()