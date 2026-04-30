"""
Unit tests for stage2_filter_expiring.py
Tests run without AWS credentials — all boto3 calls are mocked.
Run with: python -m pytest stage2_python/tests/ -v
"""

import sys
import os
import json
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, call
from io import BytesIO

# Add parent directory to path so we can import the script
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from stage2_filter_expiring import (
    has_expiring_accreditation,
    list_json_files,
    read_facility,
    write_filtered_record,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def make_cutoff(days_from_now: int = 180) -> datetime:
    """Return a timezone-aware cutoff datetime N days from now."""
    return datetime.now(tz=timezone.utc) + timedelta(days=days_from_now)


def make_facility(accreditations: list) -> dict:
    """Build a minimal facility dict with given accreditations."""
    return {
        "facility_id": "FAC_TEST",
        "facility_name": "Test Hospital",
        "employee_count": 100,
        "services": ["Emergency Care"],
        "accreditations": accreditations,
    }


# ---------------------------------------------------------------------------
# Tests for has_expiring_accreditation()
# ---------------------------------------------------------------------------
class TestHasExpiringAccreditation:

    def test_accreditation_expiring_soon_returns_true(self):
        """Accreditation expiring in 30 days should match the 180-day window."""
        expiry = (datetime.now(tz=timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%d")
        facility = make_facility([
            {"accreditation_body": "Joint Commission", "accreditation_id": "JC001", "valid_until": expiry}
        ])
        assert has_expiring_accreditation(facility, make_cutoff(180)) is True

    def test_accreditation_not_expiring_returns_false(self):
        """Accreditation expiring in 400 days should NOT match the 180-day window."""
        expiry = (datetime.now(tz=timezone.utc) + timedelta(days=400)).strftime("%Y-%m-%d")
        facility = make_facility([
            {"accreditation_body": "NCQA", "accreditation_id": "NCQA001", "valid_until": expiry}
        ])
        assert has_expiring_accreditation(facility, make_cutoff(180)) is False

    def test_multiple_accreditations_one_expiring_returns_true(self):
        """If one accreditation is expiring, function should return True even if others are not."""
        soon = (datetime.now(tz=timezone.utc) + timedelta(days=45)).strftime("%Y-%m-%d")
        late = (datetime.now(tz=timezone.utc) + timedelta(days=600)).strftime("%Y-%m-%d")
        facility = make_facility([
            {"accreditation_body": "NCQA", "accreditation_id": "NCQA001", "valid_until": late},
            {"accreditation_body": "Joint Commission", "accreditation_id": "JC001", "valid_until": soon},
        ])
        assert has_expiring_accreditation(facility, make_cutoff(180)) is True

    def test_missing_valid_until_field_is_skipped(self):
        """Accreditation entry with no valid_until should be skipped gracefully."""
        facility = make_facility([
            {"accreditation_body": "Joint Commission", "accreditation_id": "JC001"}
            # no valid_until key
        ])
        # Should not raise; returns False because no parseable date found
        assert has_expiring_accreditation(facility, make_cutoff(180)) is False

    def test_malformed_date_string_is_skipped(self):
        """Accreditation with malformed date should log a warning and be skipped."""
        facility = make_facility([
            {"accreditation_body": "NCQA", "accreditation_id": "NCQA001", "valid_until": "not-a-date"}
        ])
        # Should not raise; returns False
        assert has_expiring_accreditation(facility, make_cutoff(180)) is False

    def test_empty_accreditations_list_returns_false(self):
        """Facility with no accreditations should return False."""
        facility = make_facility([])
        assert has_expiring_accreditation(facility, make_cutoff(180)) is False

    def test_missing_accreditations_key_returns_false(self):
        """Facility dict with no 'accreditations' key at all should return False."""
        facility = {"facility_id": "FAC_NOKEY", "facility_name": "No Accreditations Facility"}
        assert has_expiring_accreditation(facility, make_cutoff(180)) is False

    def test_accreditation_expiring_exactly_on_cutoff_returns_true(self):
        """Accreditation expiring exactly on the cutoff date should match (<=)."""
        cutoff = make_cutoff(180)
        expiry = cutoff.strftime("%Y-%m-%d")
        facility = make_facility([
            {"accreditation_body": "Joint Commission", "accreditation_id": "JC001", "valid_until": expiry}
        ])
        assert has_expiring_accreditation(facility, cutoff) is True

    def test_already_expired_accreditation_returns_true(self):
        """Accreditation that already expired in the past should return True."""
        past = (datetime.now(tz=timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        facility = make_facility([
            {"accreditation_body": "NCQA", "accreditation_id": "NCQA001", "valid_until": past}
        ])
        assert has_expiring_accreditation(facility, make_cutoff(180)) is True


# ---------------------------------------------------------------------------
# Tests for list_json_files()
# ---------------------------------------------------------------------------
class TestListJsonFiles:

    def test_returns_only_json_keys(self):
        """Should only return keys ending in .json, filtering out other file types."""
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"Contents": [
                {"Key": "raw/FAC12345.json"},
                {"Key": "raw/FAC54321.json"},
                {"Key": "raw/README.txt"},   # should be excluded
                {"Key": "raw/image.png"},    # should be excluded
            ]}
        ]
        result = list_json_files(mock_s3, "test-bucket", "raw/")
        assert result == ["raw/FAC12345.json", "raw/FAC54321.json"]

    def test_handles_empty_bucket(self):
        """Empty bucket/prefix should return an empty list without error."""
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": []}]
        result = list_json_files(mock_s3, "test-bucket", "raw/")
        assert result == []

    def test_handles_multiple_pages(self):
        """Should aggregate results across multiple paginator pages."""
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"Contents": [{"Key": "raw/FAC001.json"}]},
            {"Contents": [{"Key": "raw/FAC002.json"}]},
        ]
        result = list_json_files(mock_s3, "test-bucket", "raw/")
        assert result == ["raw/FAC001.json", "raw/FAC002.json"]


# ---------------------------------------------------------------------------
# Tests for read_facility()
# ---------------------------------------------------------------------------
class TestReadFacility:

    def test_reads_valid_json(self):
        """Should correctly parse a valid JSON facility record from S3."""
        mock_s3 = MagicMock()
        facility_data = {"facility_id": "FAC001", "facility_name": "Test"}
        mock_s3.get_object.return_value = {
            "Body": BytesIO(json.dumps(facility_data).encode("utf-8"))
        }
        result = read_facility(mock_s3, "bucket", "raw/FAC001.json")
        assert result == facility_data

    def test_returns_none_on_missing_key(self):
        """Should return None when the S3 object does not exist."""
        mock_s3 = MagicMock()
        mock_s3.exceptions.NoSuchKey = Exception
        mock_s3.get_object.side_effect = mock_s3.exceptions.NoSuchKey("Not found")
        result = read_facility(mock_s3, "bucket", "raw/MISSING.json")
        assert result is None

    def test_returns_none_on_invalid_json(self):
        """Should return None when the S3 object contains malformed JSON."""
        mock_s3 = MagicMock()
        mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        mock_s3.get_object.return_value = {
            "Body": BytesIO(b"{ this is not valid json }")
        }
        result = read_facility(mock_s3, "bucket", "raw/BAD.json")
        assert result is None


# ---------------------------------------------------------------------------
# Tests for write_filtered_record()
# ---------------------------------------------------------------------------
class TestWriteFilteredRecord:

    def test_writes_to_correct_s3_key(self):
        """Should PUT object to the correct destination key."""
        mock_s3 = MagicMock()
        facility = {"facility_id": "FAC999", "facility_name": "Write Test"}
        write_filtered_record(mock_s3, facility, "dest-bucket", "filtered/")
        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "dest-bucket"
        assert call_kwargs["Key"] == "filtered/FAC999.json"
        assert call_kwargs["ContentType"] == "application/json"

    def test_written_content_is_valid_json(self):
        """Written body should be valid JSON containing the facility data."""
        mock_s3 = MagicMock()
        facility = {"facility_id": "FAC999", "facility_name": "Write Test"}
        write_filtered_record(mock_s3, facility, "dest-bucket", "filtered/")
        call_kwargs = mock_s3.put_object.call_args[1]
        parsed = json.loads(call_kwargs["Body"].decode("utf-8"))
        assert parsed["facility_id"] == "FAC999"

    def test_raises_on_s3_error(self):
        """Should propagate S3 write errors rather than swallowing them."""
        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = Exception("S3 write failure")
        facility = {"facility_id": "FAC999"}
        with pytest.raises(Exception, match="S3 write failure"):
            write_filtered_record(mock_s3, facility, "dest-bucket", "filtered/")
