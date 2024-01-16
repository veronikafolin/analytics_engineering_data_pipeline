select
    UNIQUE_ID,
	SHORT_NAME as TEST_SHORT_NAME,
	JSON_EXTRACT_PATH_TEXT(META, 'owner') as TEST_OWNERS,
	TRANSLATE(MODEL_OWNERS, '["]', '') as MODEL_OWNERS,
	DESCRIPTION,
	ORIGINAL_PATH

from {{ source('elementary', 'dbt_tests') }}