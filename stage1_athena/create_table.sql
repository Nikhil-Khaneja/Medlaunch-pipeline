-- =============================================================================
-- Stage 1: Create Athena External Table for Healthcare Facilities
-- Uses JSON SerDe to handle nested JSON structure from S3
-- Run this once in Athena Query Editor before executing the metrics query
-- =============================================================================

-- Create database (run separately if it doesn't exist)
-- CREATE DATABASE IF NOT EXISTS healthcare_db;

CREATE EXTERNAL TABLE IF NOT EXISTS healthcare_db.healthcare_facilities (
  facility_id     STRING                  COMMENT 'Unique facility identifier',
  facility_name   STRING                  COMMENT 'Name of the healthcare facility',
  employee_count  INT                     COMMENT 'Total number of employees',
  location        STRUCT<
    address: STRING,
    city:    STRING,
    state:   STRING,
    zip:     STRING
  >                                       COMMENT 'Physical location of facility',
  services        ARRAY<STRING>           COMMENT 'List of offered medical services',
  labs            ARRAY<STRUCT<
    lab_name:       STRING,
    certifications: ARRAY<STRING>
  >>                                      COMMENT 'Laboratory facilities and their certifications',
  accreditations  ARRAY<STRUCT<
    accreditation_body: STRING,
    accreditation_id:   STRING,
    valid_until:        STRING
  >>                                      COMMENT 'Accreditation records with expiry dates'
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'TRUE',
  'dots.in.keys'          = 'FALSE',
  'case.insensitive'      = 'TRUE'
)
STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://YOUR-RAW-BUCKET-NAME/raw/'
TBLPROPERTIES (
  'has_encrypted_data' = 'false',
  'classification'     = 'json'
);
