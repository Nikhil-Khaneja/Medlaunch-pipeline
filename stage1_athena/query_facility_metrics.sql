-- =============================================================================
-- Stage 1: Extract Key Facility Metrics from Healthcare JSON Data
--
-- Extracts: facility_id, facility_name, employee_count,
--           number_of_offered_services, expiry_date_of_first_accreditation
--
-- Notes:
--   - CARDINALITY() counts elements in an ARRAY type
--   - accreditations[1] accesses the first element (Presto arrays are 1-indexed)
--   - .valid_until uses dot notation to read a STRUCT field inside an ARRAY element
--   - Results ordered by expiry date ascending so soonest-to-expire appear first
--   - Output saved to S3 via Athena console Query Results settings
-- =============================================================================

SELECT
  facility_id,
  facility_name,
  employee_count,
  CARDINALITY(services)              AS number_of_offered_services,
  accreditations[1].valid_until      AS expiry_date_of_first_accreditation
FROM
  healthcare_db.healthcare_facilities
WHERE
  accreditations IS NOT NULL
  AND CARDINALITY(accreditations) > 0
ORDER BY
  expiry_date_of_first_accreditation ASC;

-- =============================================================================
-- To save results to S3, configure in Athena Settings:
--   Query result location: s3://YOUR-ATHENA-RESULTS-BUCKET/results/
-- Or use AWS CLI:
--   aws athena start-query-execution \
--     --query-string file://query_facility_metrics.sql \
--     --query-execution-context Database=healthcare_db \
--     --result-configuration OutputLocation=s3://YOUR-ATHENA-RESULTS-BUCKET/results/
-- =============================================================================
