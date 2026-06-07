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

