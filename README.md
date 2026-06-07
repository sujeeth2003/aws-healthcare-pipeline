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

