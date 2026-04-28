#!/bin/bash
# upload_sample_data.sh — Upload sample facility JSON records to S3 raw bucket
#
# Usage:
#   bash scripts/upload_sample_data.sh <BUCKET_NAME>
#   Example: bash scripts/upload_sample_data.sh medlaunch-pipeline-raw

set -euo pipefail

BUCKET_NAME="${1:?Usage: $0 <BUCKET_NAME>}"
SAMPLE_DIR="$(dirname "$0")/../data/sample"
S3_PREFIX="raw/"

echo "Uploading sample data to s3://${BUCKET_NAME}/${S3_PREFIX}"
echo "Source directory: ${SAMPLE_DIR}"
echo "---"

for FILE in "${SAMPLE_DIR}"/*.json; do
  FILENAME=$(basename "$FILE")
  S3_KEY="${S3_PREFIX}${FILENAME}"
  aws s3 cp "$FILE" "s3://${BUCKET_NAME}/${S3_KEY}" --content-type "application/json"
  echo "✓ Uploaded: s3://${BUCKET_NAME}/${S3_KEY}"
done

echo "---"
echo "Done! Verifying upload:"
aws s3 ls "s3://${BUCKET_NAME}/${S3_PREFIX}"
