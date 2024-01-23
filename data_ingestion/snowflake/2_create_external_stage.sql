GRANT USAGE ON DATABASE RAW TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA RAW.stages TO ROLE ACCOUNTADMIN;
GRANT CREATE STAGE ON SCHEMA RAW.stages TO ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION gcs_int TO ROLE ACCOUNTADMIN;

USE SCHEMA RAW.stages;

create or replace file format my_csv_format
  type = csv
  record_delimiter = '\n'
  field_delimiter = ','
  skip_header = 1
  null_if = ('NULL', 'null')
  empty_field_as_null = true
  FIELD_OPTIONALLY_ENCLOSED_BY = '0x22'

SHOW FILE FORMATS

CREATE STAGE my_gcs_stage
  URL = 'gcs://data-ingestion-tpch/'
  STORAGE_INTEGRATION = gcs_int
  FILE_FORMAT = my_csv_format;