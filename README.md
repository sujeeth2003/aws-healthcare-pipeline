# AWS Healthcare Facility Accreditation Pipeline

End-to-end AWS data pipeline for extracting and filtering healthcare facility accreditation data.

## Architecture

S3 (raw JSON) → Athena SQL (extraction) → Python/boto3 (filtering) → S3 (filtered output)

