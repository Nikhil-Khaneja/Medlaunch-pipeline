"""
Unit tests for run_metadata_logger.py
Tests run without AWS credentials — all boto3 S3 calls are mocked.
Run with: python -m pytest stage2_python/tests/ -v
"""

import sys
import os
import json
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from run_metadata_logger import RunMetadata


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
SAMPLE_CONFIG = {
    "source_bucket":      "medlaunch-pipeline-raw",
    "source_prefix":      "raw/",
    "dest_bucket":        "medlaunch-pipeline-filtered",
    "dest_prefix":        "filtered/expiring-accreditations/",
    "expiry_window_days": 180,
}


def make_finished_meta(files=3, processed=3, filtered=2, skipped=1, errors=0) -> RunMetadata:
    """Return a RunMetadata instance that has been started and finished."""
    meta = RunMetadata(config=SAMPLE_CONFIG)
    meta.start()
    meta.finish(
        files_found=files,
        processed=processed,
        filtered=filtered,
        skipped=skipped,
        errors=errors,
    )
    return meta


# ---------------------------------------------------------------------------
# Tests: RunMetadata.finish() — status determination
# ---------------------------------------------------------------------------
class TestRunMetadataStatus:

    def test_status_success_when_no_errors(self):
        """Status should be 'success' when error count is zero."""
        meta = make_finished_meta(errors=0)
        assert meta.status == "success"

    def test_status_partial_when_some_errors(self):
        """Status should be 'partial' when errors > 0 but < total files."""
        meta = make_finished_meta(files=3, processed=2, filtered=1, skipped=1, errors=1)
        assert meta.status == "partial"

    def test_status_failed_when_all_files_errored(self):
        """Status should be 'failed' when error count equals total files found."""
        meta = make_finished_meta(files=3, processed=0, filtered=0, skipped=0, errors=3)
        assert meta.status == "failed"


# ---------------------------------------------------------------------------
# Tests: RunMetadata.to_dict() — schema validation
# ---------------------------------------------------------------------------
class TestRunMetadataToDict:

    def test_to_dict_contains_required_keys(self):
        """to_dict() must include all documented top-level keys."""
        meta = make_finished_meta()
        result = meta.to_dict()
        for key in ["run_id", "pipeline", "started_at", "completed_at",
                    "duration_seconds", "status", "config", "metrics", "errors"]:
            assert key in result, f"Missing key: {key}"

    def test_metrics_values_match_finish_args(self):
        """Metrics in to_dict() must reflect the values passed to finish()."""
        meta = make_finished_meta(files=5, processed=4, filtered=3, skipped=1, errors=1)
        m = meta.to_dict()["metrics"]
        assert m["files_found"] == 5
        assert m["processed"]   == 4
        assert m["filtered"]    == 3
        assert m["skipped"]     == 1
        assert m["errors"]      == 1

    def test_config_is_embedded_in_output(self):
        """The config passed at init should appear verbatim in the output dict."""
        meta = make_finished_meta()
        assert meta.to_dict()["config"] == SAMPLE_CONFIG

    def test_run_id_starts_with_run_prefix(self):
        """run_id should follow the pattern 'run_<timestamp>'."""
        meta = make_finished_meta()
        assert meta.to_dict()["run_id"].startswith("run_")

    def test_duration_seconds_is_non_negative(self):
        """duration_seconds must be >= 0."""
        meta = make_finished_meta()
        assert meta.to_dict()["duration_seconds"] >= 0

    def test_errors_list_empty_when_no_errors_recorded(self):
        """errors list should be empty when record_error() was never called."""
        meta = make_finished_meta()
        assert meta.to_dict()["errors"] == []

    def test_pipeline_name_is_set(self):
        """Pipeline name should be the medlaunch identifier."""
        meta = make_finished_meta()
        assert meta.to_dict()["pipeline"] == "medlaunch-stage2-expiry-filter"


# ---------------------------------------------------------------------------
# Tests: RunMetadata.record_error()
# ---------------------------------------------------------------------------
class TestRecordError:

    def test_recorded_errors_appear_in_to_dict(self):
        """Errors added via record_error() should appear in the errors list."""
        meta = RunMetadata(config=SAMPLE_CONFIG)
        meta.start()
        meta.record_error("raw/bad.json", "JSONDecodeError: invalid syntax")
        meta.finish(files_found=1, processed=0, filtered=0, skipped=0, errors=1)
        errors = meta.to_dict()["errors"]
        assert len(errors) == 1
        assert errors[0]["key"]    == "raw/bad.json"
        assert errors[0]["reason"] == "JSONDecodeError: invalid syntax"

    def test_multiple_errors_all_recorded(self):
        """Multiple calls to record_error() should all be preserved."""
        meta = RunMetadata(config=SAMPLE_CONFIG)
        meta.start()
        meta.record_error("raw/a.json", "NoSuchKey")
        meta.record_error("raw/b.json", "JSONDecodeError")
        meta.finish(files_found=2, processed=0, filtered=0, skipped=0, errors=2)
        assert len(meta.to_dict()["errors"]) == 2


# ---------------------------------------------------------------------------
# Tests: RunMetadata.write_to_s3()
# ---------------------------------------------------------------------------
class TestWriteToS3:

    def test_writes_to_correct_bucket_and_key_structure(self):
        """S3 put_object should be called with the metadata bucket and a partitioned key."""
        meta = make_finished_meta()
        mock_s3 = MagicMock()

        with patch("run_metadata_logger.METADATA_BUCKET", "medlaunch-pipeline-filtered"), \
             patch("run_metadata_logger.METADATA_PREFIX", "run-metadata/"):
            uri = meta.write_to_s3(mock_s3)

        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "medlaunch-pipeline-filtered"
        assert "run-metadata/" in call_kwargs["Key"]
        assert call_kwargs["ContentType"] == "application/json"

    def test_written_body_is_valid_json(self):
        """The Body written to S3 must be valid JSON containing run summary fields."""
        meta = make_finished_meta()
        mock_s3 = MagicMock()

        with patch("run_metadata_logger.METADATA_BUCKET", "medlaunch-pipeline-filtered"), \
             patch("run_metadata_logger.METADATA_PREFIX", "run-metadata/"):
            meta.write_to_s3(mock_s3)

        body = mock_s3.put_object.call_args[1]["Body"]
        parsed = json.loads(body.decode("utf-8"))
        assert "run_id"   in parsed
        assert "status"   in parsed
        assert "metrics"  in parsed

    def test_returns_s3_uri_string(self):
        """write_to_s3() should return a string starting with 's3://'."""
        meta = make_finished_meta()
        mock_s3 = MagicMock()

        with patch("run_metadata_logger.METADATA_BUCKET", "medlaunch-pipeline-filtered"), \
             patch("run_metadata_logger.METADATA_PREFIX", "run-metadata/"):
            uri = meta.write_to_s3(mock_s3)

        assert isinstance(uri, str)
        assert uri.startswith("s3://")

    def test_raises_on_s3_write_failure(self):
        """S3 write errors should be re-raised after logging."""
        meta = make_finished_meta()
        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = Exception("S3 timeout")

        with patch("run_metadata_logger.METADATA_BUCKET", "medlaunch-pipeline-filtered"), \
             patch("run_metadata_logger.METADATA_PREFIX", "run-metadata/"):
            with pytest.raises(Exception, match="S3 timeout"):
                meta.write_to_s3(mock_s3)

    def test_key_includes_year_month_partition(self):
        """S3 key should include year= and month= partitions for queryability."""
        meta = make_finished_meta()
        mock_s3 = MagicMock()

        with patch("run_metadata_logger.METADATA_BUCKET", "medlaunch-pipeline-filtered"), \
             patch("run_metadata_logger.METADATA_PREFIX", "run-metadata/"):
            meta.write_to_s3(mock_s3)

        key = mock_s3.put_object.call_args[1]["Key"]
        assert "year=" in key
        assert "month=" in key
