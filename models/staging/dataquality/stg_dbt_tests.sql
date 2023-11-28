select
    UNIQUE_ID,
	DATABASE_NAME,
	SCHEMA_NAME,
	NAME,
	SHORT_NAME,
	ALIAS,
	TEST_COLUMN_NAME,
	SEVERITY,
	TEST_PARAMS,
	TAGS,
	MODEL_OWNERS,
	MODEL_TAGS,
	META,
	DESCRIPTION,
	ORIGINAL_PATH,
	GENERATED_AT,
	METADATA_HASH

from {{ source('elementary', 'dbt_tests') }}