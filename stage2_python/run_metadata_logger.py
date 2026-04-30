"""
Run Metadata Logger
===================
Writes a structured run_summary.json to S3 after every pipeline execution.
Provides a lightweight audit trail without requiring a database.

Output location: s3://<METADATA_BUCKET>/run-metadata/run_<timestamp>.json

Schema:
{
  "run_id":           "run_20260430T143022Z",
  "pipeline":         "medlaunch-stage2-expiry-filter",
  "started_at":       "2026-04-30T14:30:22.123456Z",
  "completed_at":     "2026-04-30T14:30:25.987654Z",
  "duration_seconds": 3.86,
  "status":           "success" | "partial" | "failed",
  "config": {
    "source_bucket":       "medlaunch-pipeline-raw",
    "source_prefix":       "raw/",
    "dest_bucket":         "medlaunch-pipeline-filtered",
    "dest_prefix":         "filtered/expiring-accreditations/",
    "expiry_window_days":  180
  },
  "metrics": {
    "files_found":    3,
    "processed":      3,
    "filtered":       2,
    "skipped":        1,
    "errors":         0
  },
  "errors": []          # list of { "key": "...", "reason": "..." }
}
"""

import boto3
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, List, Optional

logger = logging.getLogger(__name__)

METADATA_BUCKET = os.environ.get("METADATA_BUCKET", os.environ.get("DEST_BUCKET", "medlaunch-pipeline-filtered"))
METADATA_PREFIX = os.environ.get("METADATA_PREFIX", "run-metadata/")
PIPELINE_NAME   = "medlaunch-stage2-expiry-filter"


class RunMetadata:
    """
    Collects pipeline run statistics and writes a summary JSON to S3.

    Usage:
        meta = RunMetadata(config={...})
        meta.start()
        # ... pipeline work ...
        meta.record_error("raw/bad.json", "JSONDecodeError: ...")
        meta.finish(files_found=3, processed=3, filtered=2, skipped=1, errors=1)
        meta.write_to_s3(s3_client)
    """

    def __init__(self, config: dict) -> None:
        """
        Initialise with pipeline config snapshot.

        Args:
            config: Dict of config values to embed in the summary
                    (bucket names, prefix, expiry window, etc.)
        """
        self.config        = config
        self.started_at: Optional[datetime]    = None
        self.completed_at: Optional[datetime]  = None
        self.status        = "pending"
        self.files_found   = 0
        self.processed     = 0
        self.filtered      = 0
        self.skipped       = 0
        self.errors        = 0
        self._error_detail: List[dict] = []

    def start(self) -> None:
        """Mark the start time of the pipeline run."""
        self.started_at = datetime.now(tz=timezone.utc)
        logger.info(f"[Metadata] Run started at {self.started_at.isoformat()}")

    def record_error(self, key: str, reason: str) -> None:
        """
        Record a per-file error for inclusion in the summary.

        Args:
            key:    S3 object key that caused the error
            reason: Human-readable error description
        """
        self._error_detail.append({"key": key, "reason": reason})

    def finish(
        self,
        files_found: int,
        processed: int,
        filtered: int,
        skipped: int,
        errors: int,
    ) -> None:
        """
        Finalise metrics and determine overall run status.

        Status rules:
          - "success"  → errors == 0
          - "partial"  → 0 < errors < files_found  (some succeeded)
          - "failed"   → errors == files_found      (nothing processed cleanly)

        Args:
            files_found: Total JSON files discovered in source prefix
            processed:   Files successfully read and evaluated
            filtered:    Files written to destination (had expiring accreditation)
            skipped:     Files with no expiring accreditations (not written)
            errors:      Files that could not be read or written
        """
        self.completed_at  = datetime.now(tz=timezone.utc)
        self.files_found   = files_found
        self.processed     = processed
        self.filtered      = filtered
        self.skipped       = skipped
        self.errors        = errors

        if errors == 0:
            self.status = "success"
        elif errors < files_found:
            self.status = "partial"
        else:
            self.status = "failed"

        duration = self._duration_seconds()
        logger.info(
            f"[Metadata] Run finished — status={self.status} "
            f"processed={processed} filtered={filtered} errors={errors} "
            f"duration={duration:.2f}s"
        )

    def _duration_seconds(self) -> float:
        """Return elapsed time in seconds, or 0.0 if timing data is missing."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0.0

    def _run_id(self) -> str:
        """Generate a unique run ID based on the start timestamp."""
        ts = self.started_at or datetime.now(tz=timezone.utc)
        return f"run_{ts.strftime('%Y%m%dT%H%M%SZ')}"

    def to_dict(self) -> dict:
        """
        Serialise the run summary to a plain dict ready for JSON encoding.

        Returns:
            Dict matching the documented schema at the top of this module
        """
        return {
            "run_id":           self._run_id(),
            "pipeline":         PIPELINE_NAME,
            "started_at":       self.started_at.isoformat() if self.started_at else None,
            "completed_at":     self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": round(self._duration_seconds(), 3),
            "status":           self.status,
            "config":           self.config,
            "metrics": {
                "files_found": self.files_found,
                "processed":   self.processed,
                "filtered":    self.filtered,
                "skipped":     self.skipped,
                "errors":      self.errors,
            },
            "errors": self._error_detail,
        }

    def write_to_s3(self, s3: Any) -> str:
        """
        Write the run summary JSON to S3 under the metadata prefix.

        The key is time-partitioned for easy querying:
            run-metadata/year=2026/month=04/run_20260430T143022Z.json

        Args:
            s3: boto3 S3 client

        Returns:
            Full S3 URI of the written object

        Raises:
            Exception: Re-raises any S3 write failure after logging
        """
        ts        = self.started_at or datetime.now(tz=timezone.utc)
        partition = f"year={ts.year}/month={ts.strftime('%m')}/"
        key       = f"{METADATA_PREFIX}{partition}{self._run_id()}.json"
        body      = json.dumps(self.to_dict(), indent=2).encode("utf-8")

        try:
            s3.put_object(
                Bucket=METADATA_BUCKET,
                Key=key,
                Body=body,
                ContentType="application/json",
            )
            uri = f"s3://{METADATA_BUCKET}/{key}"
            logger.info(f"[Metadata] Run summary written → {uri}")
            return uri
        except Exception as exc:
            logger.error(f"[Metadata] Failed to write run summary to S3: {exc}")
            raise
