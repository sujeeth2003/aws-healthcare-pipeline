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

