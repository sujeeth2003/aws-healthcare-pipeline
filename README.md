# AWS Healthcare Facility Accreditation Pipeline

End-to-end serverless data pipeline on AWS for processing and analyzing healthcare facility accreditation data.

## Architecture

```
S3 (raw JSON upload)
    → Lambda (triggered on upload)
        → Athena SQL (count accredited facilities per state)
            → Success: copy results to S3 production/
            → Failure: SNS email alert
Step Functions orchestrates the full flow with retries and error handling.
```

## Stages Completed

### Stage 1 — Data Extraction with Athena
- Created external table over S3-hosted NDJSON using OpenX JSON SerDe to handle nested JSON structure
- Query 1: extracts `facility_id`, `facility_name`, `employee_count`, service count, and first accreditation expiry date per facility
- Query 2: counts accredited facilities grouped by state
- Results saved automatically to S3 `query-results/` prefix

### Stage 2 — Data Processing with Python
- `boto3` script reads NDJSON records from S3 line by line
- Filters any facility with at least one accreditation expiring within 6 months from run date
- Writes filtered records to separate S3 prefix as NDJSON
- Full error handling: S3 access errors, malformed JSON lines, unparseable dates, missing fields — all caught and logged without crashing

### Stage 3 — Event-Driven Processing with Lambda
- Lambda function triggers automatically on new `.json` uploads to `raw-data/` S3 prefix
- Runs Athena count-by-state query, polls for completion with timeout guard (55s max to stay within Lambda limit)
- On success: copies results CSV to named output location in S3
- On failure: raises exception with structured logging for CloudWatch

### Stage 4 — Workflow Orchestration with Step Functions
- Standard state machine triggered manually (or via S3 EventBridge rule)
- Flow: Invoke Lambda → wait → on success copy results to `production/` → on failure publish SNS alert
- Retry logic on Lambda invocation errors (2 retries, exponential backoff)
- Catch-all error handler routes any failure to SNS notification before terminal Fail state
- Least-privilege IAM roles applied throughout

## Stage Selection Rationale

All four stages were completed. SQL + Python (Stages 1 + 2) cover the core data engineering pipeline — schema-on-read querying over nested JSON and programmatic record filtering. Lambda + Step Functions (Stages 3 + 4) add production-grade automation — event-driven execution, polling, retry logic, and alerting. Together they demonstrate a complete serverless pipeline from raw data ingestion to production output.

## Repository Structure

```
aws-healthcare-pipeline/
│
├── README.md
├── architecture.png
│
├── queries.sql
│
├── filter_expiring.py
│
├── lambda_function.py
│
├── state_machine.json
│
└── facilities.json
```

## Setup & Running Locally (Stage 2)

Install dependencies:
```bash
pip install boto3 python-dateutil
```

Configure AWS credentials on Windows — create this file:
```
C:\Users\USERNAME\.aws\credentials
```
With this content:
```
[default]
aws_access_key_id = YOUR_KEY
aws_secret_access_key = YOUR_SECRET
region = us-east-1
```

Update `BUCKET_NAME` in `filter_expiring.py` to your bucket name, then run:
```bash
python stage2-python/filter_expiring.py
```

Expected output:
```
INFO  Today: 2026-xx-xx | Cutoff (6 months out): 2026-xx-xx
INFO  Loaded 3 facility records
INFO  Facilities with accreditations expiring within 6 months: 2 / 3
INFO    → FAC54321 — Green Valley Clinic
INFO    → FAC67890 — Lakeside Medical Center
INFO  Writing 2 records to s3://your-bucket/filtered-output/expiring_facilities.json
INFO  Upload successful
```

## Data Format

Input data is NDJSON (one JSON object per line) stored in S3. Each facility record contains:

```json
{
  "facility_id": "FAC12345",
  "facility_name": "City Hospital",
  "location": { "state": "TX", ... },
  "employee_count": 250,
  "services": ["Emergency Care", "Surgery", ...],
  "accreditations": [
    { "accreditation_body": "Joint Commission", "valid_until": "2026-12-31" }
  ]
}
```

Athena reads this using OpenX JSON SerDe with STRUCT and ARRAY column types to handle the nested structure.

## AWS Services Used

| Service | Purpose |
|---|---|
| S3 | Raw data storage, query results, production output |
| Athena | SQL queries over nested JSON |
| Lambda | Event-driven query execution on S3 upload |
| Step Functions | Pipeline orchestration with error handling |
| SNS | Failure email alerts |
| IAM | Least-privilege roles for each service |
| CloudWatch | Lambda execution logs and monitoring |

## S3 Bucket Structure

```
your-bucket/
├── raw-data/                    ← input NDJSON files
├── query-results/               ← Athena query output (auto-generated)
├── filtered-output/             ← Stage 2 filtered records
├── athena-state-counts/         ← Stage 3 Lambda output
└── production/                  ← Stage 4 final production output
```

## Security Considerations

- Separate IAM roles for Lambda and Step Functions with only required permissions
- No credentials hardcoded anywhere — IAM roles in AWS, credentials file locally
- S3 bucket private with no public access enabled
- Lambda follows least-privilege: only S3 and Athena access granted

## Cost

All services used are within AWS Free Tier limits:
- Lambda: 1M requests/month free
- Athena: $5/TB scanned (test data is kilobytes — effectively $0)
- S3: 5GB storage free
- Step Functions: 4,000 state transitions/month free
- SNS: 1M requests/month free

See `billing-screenshot.png` for actual AWS cost during this project.
