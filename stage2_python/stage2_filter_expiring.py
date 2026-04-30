"""
Stage 2: Healthcare Facility Expiring Accreditation Filter
==========================================================
Reads JSON facility records from S3, filters those with any accreditation
expiring within 6 months, and writes matching records to a separate S3 location.

After every run a structured run_summary.json is written to S3 via
run_metadata_logger.RunMetadata — providing a full audit trail of every
execution without requiring a database.

Usage:
    export SOURCE_BUCKET=medlaunch-pipeline-nk-raw
    export DEST_BUCKET=medlaunch-pipeline-nk-filtered
    export AWS_REGION=us-east-1
    python stage2_filter_expiring.py
"""

import boto3
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional

# Load local environment overrides if present — never committed to version control
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from run_metadata_logger import RunMetadata

# ---------------------------------------------------------------------------
# Configuration — all values from environment variables
# ---------------------------------------------------------------------------
SOURCE_BUCKET      = os.environ.get("SOURCE_BUCKET", "medlaunch-pipeline-raw")
SOURCE_PREFIX      = os.environ.get("SOURCE_PREFIX", "raw/")
DEST_BUCKET        = os.environ.get("DEST_BUCKET",   "medlaunch-pipeline-filtered")
DEST_PREFIX        = os.environ.get("DEST_PREFIX",   "filtered/expiring-accreditations/")
AWS_REGION         = os.environ.get("AWS_REGION",    "us-east-1")
EXPIRY_WINDOW_DAYS = int(os.environ.get("EXPIRY_WINDOW_DAYS", "180"))  # 6 months

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_s3_client() -> Any:
    """Create and return a boto3 S3 client for the configured region."""
    return boto3.client("s3", region_name=AWS_REGION)


def list_json_files(s3: Any, bucket: str, prefix: str) -> List[str]:
    """
    List all .json object keys under the given S3 bucket/prefix.

    Uses a paginator to handle buckets with more than 1000 objects.

    Args:
        s3:     boto3 S3 client
        bucket: S3 bucket name
        prefix: key prefix to search under

    Returns:
        List of S3 object keys ending in .json
    """
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])

    logger.info(f"Found {len(keys)} JSON file(s) in s3://{bucket}/{prefix}")
    return keys


def read_facility(s3: Any, bucket: str, key: str) -> Optional[dict]:
    """
    Download and parse a single facility JSON record from S3.

    Args:
        s3:     boto3 S3 client
        bucket: S3 bucket name
        key:    S3 object key

    Returns:
        Parsed facility dict, or None if read/parse fails
    """
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read().decode("utf-8")
        return json.loads(body)
    except s3.exceptions.NoSuchKey:
        logger.error(f"File not found: s3://{bucket}/{key}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error in s3://{bucket}/{key}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error reading s3://{bucket}/{key}: {e}")
        return None


def has_expiring_accreditation(facility: dict, cutoff: datetime) -> bool:
    """
    Check if any accreditation in a facility record expires on or before the cutoff.

    Args:
        facility: Parsed facility dict (may contain an 'accreditations' list)
        cutoff:   Timezone-aware datetime; accreditations expiring by this date match

    Returns:
        True if any accreditation's valid_until <= cutoff, False otherwise
    """
    accreditations = facility.get("accreditations", [])
    if not accreditations:
        return False

    for acc in accreditations:
        valid_until_str = acc.get("valid_until", "")
        if not valid_until_str:
            logger.warning(
                f"Facility {facility.get('facility_id', 'UNKNOWN')} has accreditation "
                f"with missing valid_until field — skipping this entry"
            )
            continue

        try:
            valid_until = datetime.strptime(valid_until_str, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            if valid_until <= cutoff:
                logger.debug(
                    f"Facility {facility.get('facility_id')} has expiring accreditation: "
                    f"{acc.get('accreditation_body')} expires {valid_until_str}"
                )
                return True
        except ValueError:
            logger.warning(
                f"Facility {facility.get('facility_id', 'UNKNOWN')}: "
                f"bad date format '{valid_until_str}' — expected YYYY-MM-DD, skipping"
            )

    return False


def write_filtered_record(
    s3: Any, facility: dict, dest_bucket: str, dest_prefix: str
) -> None:
    """
    Write a filtered facility record as a JSON file to the destination S3 location.

    Args:
        s3:          boto3 S3 client
        facility:    Parsed facility dict to write
        dest_bucket: Destination S3 bucket name
        dest_prefix: Destination S3 key prefix
    """
    facility_id = facility.get("facility_id", "UNKNOWN")
    dest_key = f"{dest_prefix}{facility_id}.json"

    try:
        s3.put_object(
            Bucket=dest_bucket,
            Key=dest_key,
            Body=json.dumps(facility, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info(f"Written filtered record → s3://{dest_bucket}/{dest_key}")
    except Exception as e:
        logger.error(f"Failed to write facility {facility_id} to S3: {e}")
        raise


def main() -> None:
    """Main pipeline entry point — orchestrates list, filter, write, and metadata logging."""
    logger.info("=" * 60)
    logger.info("Stage 2: MedLaunch Facility Expiring Accreditation Filter")
    logger.info("=" * 60)
    logger.info(f"Source : s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}")
    logger.info(f"Dest   : s3://{DEST_BUCKET}/{DEST_PREFIX}")
    logger.info(f"Region : {AWS_REGION}")

    now    = datetime.now(tz=timezone.utc)
    cutoff = now + timedelta(days=EXPIRY_WINDOW_DAYS)
    logger.info(f"Cutoff : {cutoff.date()} (accreditations expiring within {EXPIRY_WINDOW_DAYS} days)")

    # Initialise metadata tracker — captures config snapshot at run start
    meta = RunMetadata(config={
        "source_bucket":      SOURCE_BUCKET,
        "source_prefix":      SOURCE_PREFIX,
        "dest_bucket":        DEST_BUCKET,
        "dest_prefix":        DEST_PREFIX,
        "expiry_window_days": EXPIRY_WINDOW_DAYS,
    })
    meta.start()

    s3   = get_s3_client()
    keys = list_json_files(s3, SOURCE_BUCKET, SOURCE_PREFIX)

    processed = 0
    filtered  = 0
    errors    = 0

    for key in keys:
        facility = read_facility(s3, SOURCE_BUCKET, key)
        if facility is None:
            errors += 1
            meta.record_error(key, "Failed to read or parse JSON record")
            continue

        processed += 1
        facility_id = facility.get("facility_id", "UNKNOWN")

        if has_expiring_accreditation(facility, cutoff):
            try:
                write_filtered_record(s3, facility, DEST_BUCKET, DEST_PREFIX)
                filtered += 1
            except Exception as exc:
                errors += 1
                meta.record_error(key, f"S3 write failure for {facility_id}: {exc}")
        else:
            logger.info(f"Facility {facility_id} — no expiring accreditations, skipped")

    skipped = processed - filtered

    # Finalise and persist run summary to S3
    meta.finish(
        files_found=len(keys),
        processed=processed,
        filtered=filtered,
        skipped=skipped,
        errors=errors,
    )
    try:
        summary_uri = meta.write_to_s3(s3)
        logger.info(f"Run summary → {summary_uri}")
    except Exception:
        logger.warning("Run summary could not be written to S3 — pipeline results are still valid")

    logger.info("=" * 60)
    logger.info(f"SUMMARY: Processed={processed} | Filtered={filtered} | Skipped={skipped} | Errors={errors}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
