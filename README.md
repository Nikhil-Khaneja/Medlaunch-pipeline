# MedLaunch — Healthcare Facility Analytics Pipeline

End-to-end healthcare analytics pipeline on AWS — automatically surfaces facilities
with expiring accreditations using serverless Athena queries and Python filtering on S3.

## Architecture

```
S3 Raw Bucket (raw/*.json)
        │
        ├──► AWS Glue Data Catalog (healthcare_db schema)
        │              │
        │              ▼
        │         Amazon Athena  ──► S3 Athena Results (athena-results/)
        │         (Stage 1 SQL)
        │
        └──► Python / boto3 script (Stage 2)
                       │
                       ▼ Filter: any accreditation expiring within 6 months
                       │
                  S3 Filtered Bucket (filtered/expiring-accreditations/)
                       │
                       ▼
                  Run Audit Log (run-metadata/year=.../month=.../run_*.json)

IAM Role: medlaunch-pipeline-nk-pipeline-role (least-privilege, scoped to 3 buckets)
Infrastructure: AWS CloudFormation
```

## Stages Implemented

### Stage 1 — Data Extraction with Athena (SQL)
Registers an Athena external table over raw S3 JSON using the JSON SerDe, then
queries facility metrics: id, name, employee count, number of offered services, and
the expiry date of the first accreditation. Results saved automatically to S3.

### Stage 2 — Data Processing with Python (boto3)
Reads all facility records from the raw S3 bucket, filters any facility with
an accreditation expiring within 180 days, and writes matching records to a
separate S3 prefix. Includes full error handling, structured logging, run audit
trail, and unit tests.

## Stage Selection Rationale

Stages 1 and 2 were chosen because together they demonstrate the full data
engineering lifecycle — from cloud-native serverless querying of raw nested JSON
at scale with Athena, to programmatic filtering and transformation with Python.
This combination showcases two distinct and complementary skill sets: SQL fluency
with Presto/Athena idioms and nested JSON SerDe patterns, alongside production-quality
Python with proper error handling, boto3 paginators for scale, and comprehensive
unit testing without cloud dependencies.

## Repository Structure

```
medlaunch-pipeline/
├── README.md
├── .gitignore
├── .env.example                          # Config template (no credentials)
├── stage1_athena/
│   ├── create_table.sql                  # Athena external table DDL
│   └── query_facility_metrics.sql        # Facility metrics extraction query
├── stage2_python/
│   ├── stage2_filter_expiring.py         # Main boto3 pipeline script
│   ├── run_metadata_logger.py            # Run audit trail module
│   ├── requirements.txt
│   └── tests/
│       ├── test_filter.py                # 18 unit tests for filter logic
│       └── test_run_metadata_logger.py   # 17 unit tests for metadata logger
├── data/
│   └── sample/
│       ├── FAC12345.json
│       ├── FAC54321.json
│       └── FAC67890.json
├── infrastructure/
│   └── cloudformation.yaml              # S3 buckets + IAM role
└── scripts/
    ├── upload_sample_data.sh            # Upload sample records to S3
    └── functional_test.sh              # End-to-end functional test suite
```

## Prerequisites

- AWS account with CLI configured (`aws configure`)
- Python 3.11+
- `pip install -r stage2_python/requirements.txt`

## Setup & Deployment

### Step 1 — Deploy AWS Infrastructure

Log into the AWS Console and navigate to **CloudFormation → Create Stack**.
Upload `infrastructure/cloudformation.yaml` and set the `BucketNamePrefix`
parameter to `medlaunch-pipeline-nk`. The stack creates:

- `medlaunch-pipeline-nk-raw` — source bucket for facility JSON records
- `medlaunch-pipeline-nk-filtered` — destination for expiring accreditation records
- `medlaunch-pipeline-nk-athena-results` — Athena query output location
- IAM role with least-privilege access scoped to the above three buckets

Alternatively via AWS CLI:

```bash
aws cloudformation create-stack \
  --stack-name medlaunch-pipeline \
  --template-body file://infrastructure/cloudformation.yaml \
  --parameters ParameterKey=BucketNamePrefix,ParameterValue=medlaunch-pipeline-nk \
  --capabilities CAPABILITY_NAMED_IAM

aws cloudformation wait stack-create-complete --stack-name medlaunch-pipeline
```

### Step 2 — Upload Sample Data

```bash
bash scripts/upload_sample_data.sh medlaunch-pipeline-nk-raw
```

### Step 3 — Stage 1: Register Athena Table and Run Query

1. Open **AWS Athena Console → Query Editor**
2. Go to **Settings** → set Query result location:
   `s3://medlaunch-pipeline-nk-athena-results/results/`
3. Create the database:
   ```sql
   CREATE DATABASE IF NOT EXISTS healthcare_db;
   ```
4. Run `stage1_athena/create_table.sql` (update the LOCATION bucket name)
5. Run `stage1_athena/query_facility_metrics.sql`
6. Results are saved automatically to the Athena results bucket

### Step 4 — Stage 2: Run the Python Filter Pipeline

```bash
pip install -r stage2_python/requirements.txt

export SOURCE_BUCKET=medlaunch-pipeline-nk-raw
export DEST_BUCKET=medlaunch-pipeline-nk-filtered
export AWS_REGION=us-east-1

python stage2_python/stage2_filter_expiring.py
```

Verify filtered output:
```bash
aws s3 ls s3://medlaunch-pipeline-nk-filtered/filtered/expiring-accreditations/
```

Verify run audit trail:
```bash
aws s3 ls s3://medlaunch-pipeline-nk-filtered/run-metadata/ --recursive
```

### Step 5 — Run Unit Tests

```bash
python -m pytest stage2_python/tests/ -v
# 35 tests — no AWS credentials required
```

### Step 6 — Run Functional Tests (requires AWS)

```bash
export SOURCE_BUCKET=medlaunch-pipeline-nk-raw
export DEST_BUCKET=medlaunch-pipeline-nk-filtered
export AWS_REGION=us-east-1

bash scripts/functional_test.sh
# 7 end-to-end tests against real AWS resources
```

## AWS Best Practices Applied

- **Infrastructure as Code** — all resources provisioned via CloudFormation, zero manual console clicks
- **IAM Least Privilege** — role policy scoped to exact bucket ARNs, no wildcard resources
- **S3 Versioning** — enabled on raw bucket for data recovery
- **Boto3 Paginators** — all S3 list operations paginated, handles any dataset size
- **Error Handling** — every failure caught, logged with context, and counted in the run summary
- **Run Audit Trail** — structured JSON summary written to S3 after every execution, time-partitioned for queryability
- **Unit Tests** — 35 tests covering all core logic using mocks, no cloud credentials needed
- **Functional Tests** — 7 end-to-end tests verify real AWS behaviour after deployment

## Cost Estimate (AWS Free Tier)

| Service        | Usage             | Free Tier Limit   | Cost   |
|----------------|-------------------|-------------------|--------|
| S3             | ~10KB, ~50 req    | 5GB / 20K GETs/mo | $0     |
| Athena         | ~1KB scanned      | 1TB/mo            | $0     |
| CloudFormation | 1 stack           | Always free       | $0     |
| IAM            | 1 role, 1 policy  | Always free       | $0     |
| **Total**      |                   |                   | **$0** |

## Cleanup

```bash
aws s3 rm s3://medlaunch-pipeline-nk-raw --recursive
aws s3 rm s3://medlaunch-pipeline-nk-filtered --recursive
aws s3 rm s3://medlaunch-pipeline-nk-athena-results --recursive
aws cloudformation delete-stack --stack-name medlaunch-pipeline
```
