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

