#!/bin/bash
# =============================================================================
# functional_test.sh — End-to-end functional test for the MedLaunch pipeline
#
# Tests the full pipeline against real AWS resources:
#   1. Uploads sample data to the raw S3 bucket
#   2. Runs the Stage 2 Python filter script
#   3. Verifies filtered records appear in the destination bucket
#   4. Verifies run_summary.json was written to the metadata prefix
#   5. Validates the run_summary.json structure
#   6. Prints a PASS / FAIL summary
#
# Usage:
#   export SOURCE_BUCKET=medlaunch-pipeline-nk-raw
#   export DEST_BUCKET=medlaunch-pipeline-nk-filtered
#   export AWS_REGION=us-east-1
#   bash scripts/functional_test.sh
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SOURCE_BUCKET="${SOURCE_BUCKET:-medlaunch-pipeline-nk-raw}"
DEST_BUCKET="${DEST_BUCKET:-medlaunch-pipeline-nk-filtered}"
METADATA_BUCKET="${METADATA_BUCKET:-$DEST_BUCKET}"
AWS_REGION="${AWS_REGION:-us-east-1}"
SAMPLE_DIR="$(dirname "$0")/../data/sample"
SCRIPT_DIR="$(dirname "$0")/../stage2_python"

PASS=0
FAIL=0
RESULTS=()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log()    { echo -e "${YELLOW}[TEST]${NC} $1"; }
pass()   { echo -e "${GREEN}[PASS]${NC} $1"; PASS=$((PASS+1)); RESULTS+=("PASS: $1"); }
fail()   { echo -e "${RED}[FAIL]${NC} $1"; FAIL=$((FAIL+1)); RESULTS+=("FAIL: $1"); }

check_aws_cli() {
  if ! command -v aws &>/dev/null; then
    echo -e "${RED}ERROR:${NC} AWS CLI not found. Install with: pip install awscli"
    exit 1
  fi
  if ! aws sts get-caller-identity --region "$AWS_REGION" &>/dev/null; then
    echo -e "${RED}ERROR:${NC} AWS credentials not configured or invalid."
    exit 1
  fi
  echo -e "${GREEN}AWS CLI authenticated successfully${NC}"
}

check_python() {
  if ! command -v python3 &>/dev/null; then
    echo -e "${RED}ERROR:${NC} Python 3 not found."
    exit 1
  fi
}

# ---------------------------------------------------------------------------
# Test 1: Upload sample data to raw bucket
# ---------------------------------------------------------------------------
test_upload_sample_data() {
  log "Test 1: Uploading sample data to s3://${SOURCE_BUCKET}/raw/"
  local uploaded=0
  for FILE in "${SAMPLE_DIR}"/*.json; do
    FILENAME=$(basename "$FILE")
    if aws s3 cp "$FILE" "s3://${SOURCE_BUCKET}/raw/${FILENAME}" \
         --content-type "application/json" \
         --region "$AWS_REGION" &>/dev/null; then
      uploaded=$((uploaded+1))
    fi
  done
  if [ "$uploaded" -ge 3 ]; then
    pass "Uploaded ${uploaded} sample JSON files to raw bucket"
  else
    fail "Only ${uploaded}/3 sample files uploaded"
  fi
}

# ---------------------------------------------------------------------------
# Test 2: Raw files are visible in S3
# ---------------------------------------------------------------------------
test_raw_files_exist() {
  log "Test 2: Verifying raw files exist in S3"
  local count
  count=$(aws s3 ls "s3://${SOURCE_BUCKET}/raw/" --region "$AWS_REGION" \
          | grep -c "\.json" || true)
  if [ "$count" -ge 3 ]; then
    pass "Found ${count} JSON files in s3://${SOURCE_BUCKET}/raw/"
  else
    fail "Expected >= 3 JSON files in raw/, found ${count}"
  fi
}

# ---------------------------------------------------------------------------
# Test 3: Run Stage 2 Python pipeline
# ---------------------------------------------------------------------------
test_run_pipeline() {
  log "Test 3: Running Stage 2 Python pipeline"
  export SOURCE_BUCKET DEST_BUCKET METADATA_BUCKET AWS_REGION

  local output
  if output=$(cd "$SCRIPT_DIR" && python3 stage2_filter_expiring.py 2>&1); then
    if echo "$output" | grep -q "SUMMARY:"; then
      pass "Pipeline ran successfully and printed SUMMARY"
    else
      fail "Pipeline ran but SUMMARY line not found in output"
    fi
    echo "$output" | grep -E "\[INFO\]|\[WARNING\]|\[ERROR\]|SUMMARY" | head -20
  else
    fail "Pipeline script exited with non-zero status"
    echo "$output"
  fi
}

# ---------------------------------------------------------------------------
# Test 4: Filtered records appear in destination bucket
# ---------------------------------------------------------------------------
test_filtered_records_exist() {
  log "Test 4: Verifying filtered records in destination bucket"
  local count
  count=$(aws s3 ls "s3://${DEST_BUCKET}/filtered/expiring-accreditations/" \
          --region "$AWS_REGION" | grep -c "\.json" || true)
  if [ "$count" -ge 1 ]; then
    pass "Found ${count} filtered record(s) in s3://${DEST_BUCKET}/filtered/expiring-accreditations/"
  else
    fail "No filtered records found — expected at least 1 expiring facility"
  fi
}

# ---------------------------------------------------------------------------
# Test 5: Run metadata summary was written
# ---------------------------------------------------------------------------
test_metadata_written() {
  log "Test 5: Verifying run_summary.json written to metadata prefix"
  local count
  count=$(aws s3 ls "s3://${METADATA_BUCKET}/run-metadata/" \
          --recursive --region "$AWS_REGION" | grep -c "\.json" || true)
  if [ "$count" -ge 1 ]; then
    pass "Found ${count} run summary file(s) in s3://${METADATA_BUCKET}/run-metadata/"
  else
    fail "No run_summary.json found in run-metadata/ prefix"
  fi
}

# ---------------------------------------------------------------------------
# Test 6: Validate run_summary.json structure
# ---------------------------------------------------------------------------
test_metadata_structure() {
  log "Test 6: Validating run_summary.json structure"

  # Get the latest run summary key
  local key
  key=$(aws s3 ls "s3://${METADATA_BUCKET}/run-metadata/" \
        --recursive --region "$AWS_REGION" \
        | sort | tail -1 | awk '{print $4}')

  if [ -z "$key" ]; then
    fail "Could not find any run summary file to validate"
    return
  fi

  # Download and validate JSON structure
  local tmpfile="/tmp/run_summary_test_$$.json"
  aws s3 cp "s3://${METADATA_BUCKET}/${key}" "$tmpfile" \
      --region "$AWS_REGION" &>/dev/null

  local required_keys=("run_id" "pipeline" "started_at" "completed_at" "status" "metrics" "errors")
  local all_present=true

  for k in "${required_keys[@]}"; do
    if ! python3 -c "import json,sys; d=json.load(open('$tmpfile')); assert '$k' in d" 2>/dev/null; then
      fail "run_summary.json missing required key: '$k'"
      all_present=false
    fi
  done

  if $all_present; then
    local status
    status=$(python3 -c "import json; print(json.load(open('$tmpfile'))['status'])")
    local processed
    processed=$(python3 -c "import json; print(json.load(open('$tmpfile'))['metrics']['processed'])")
    local filtered
    filtered=$(python3 -c "import json; print(json.load(open('$tmpfile'))['metrics']['filtered'])")
    pass "run_summary.json structure valid — status=${status} processed=${processed} filtered=${filtered}"
  fi

  rm -f "$tmpfile"
}

# ---------------------------------------------------------------------------
# Test 7: Confirm no unintended files in wrong locations
# ---------------------------------------------------------------------------
test_no_data_leakage() {
  log "Test 7: Checking filtered records are NOT in the raw prefix"
  local count
  count=$(aws s3 ls "s3://${SOURCE_BUCKET}/filtered/" \
          --region "$AWS_REGION" 2>/dev/null | wc -l || echo 0)
  if [ "$count" -eq 0 ]; then
    pass "No filtered records found in raw bucket — data isolation confirmed"
  else
    fail "Unexpected files found under s3://${SOURCE_BUCKET}/filtered/ — check pipeline logic"
  fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  MedLaunch Pipeline — Functional Test Suite"
echo "  Source : s3://${SOURCE_BUCKET}"
echo "  Dest   : s3://${DEST_BUCKET}"
echo "  Region : ${AWS_REGION}"
echo "============================================================"
echo ""

check_aws_cli
check_python

echo ""
test_upload_sample_data
test_raw_files_exist
test_run_pipeline
test_filtered_records_exist
test_metadata_written
test_metadata_structure
test_no_data_leakage

echo ""
echo "============================================================"
echo "  RESULTS"
echo "============================================================"
for r in "${RESULTS[@]}"; do
  if [[ "$r" == PASS* ]]; then
    echo -e "  ${GREEN}${r}${NC}"
  else
    echo -e "  ${RED}${r}${NC}"
  fi
done
echo ""
echo -e "  Tests passed : ${GREEN}${PASS}${NC}"
echo -e "  Tests failed : ${RED}${FAIL}${NC}"
echo "============================================================"
echo ""

if [ "$FAIL" -eq 0 ]; then
  echo -e "${GREEN}ALL TESTS PASSED — Pipeline is working correctly${NC}"
  exit 0
else
  echo -e "${RED}${FAIL} TEST(S) FAILED — Review output above${NC}"
  exit 1
fi
