<<<<<<< HEAD
# AWS Healthcare Facility Accreditation Pipeline

End-to-end AWS data pipeline for extracting and filtering healthcare facility accreditation data.

## Architecture

S3 (raw JSON) → Athena SQL (extraction) → Python/boto3 (filtering) → S3 (filtered output)

## Stages Completed

**Stage 1: Data Extraction with Athena**
- Created external table over S3-hosted NDJSON using OpenX JSON SerDe
- Queried nested JSON to extract facility_id, name, employee_count, service count, first accreditation expiry, and state
- Ran aggregation query counting accredited facilities per state
- Results saved to S3 query-results prefix

**Stage 2: Data Processing with Python**
- boto3 script reads NDJSON records from S3 line by line
- Filters facilities with any accreditation expiring within 6 months from today
- Writes filtered records to separate S3 prefix as NDJSON
- Full error handling: missing keys, malformed JSON, S3 read/write errors, unparseable dates
- Structured logging for auditability

## Stage Selection Rationale

I chose SQL + Python because it demonstrates end-to-end data engineering: raw nested JSON extraction via Athena and programmatic filtering/routing via boto3. Together they cover the core pipeline skills — schema-on-read querying, conditional data transformation, and S3-based data movement — that map most directly to the healthcare analytics use case.

## Setup

```bash
pip install boto3 python-dateutil
aws configure   # enter your credentials
python filter_expiring.py
```

## Files

- `filter_expiring.py` — Stage 2: Python filtering script
- `facilities.json` — Sample NDJSON dataset (3 facilities)
- Athena queries in `athena_queries.sql`
=======
# aws-healthcare-pipeline
>>>>>>> origin/main
