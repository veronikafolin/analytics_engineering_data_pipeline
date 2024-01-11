select

	TEST_UNIQUE_ID,
	MODEL_UNIQUE_ID,
	DATABASE_NAME,
	SCHEMA_NAME,
	TABLE_NAME,
	NVL(COLUMN_NAME, 'combination_of_columns') as COLUMN_NAME,
	DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME  AS TABLE_REF,
	DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME || '.' || NVL(COLUMN_NAME, 'combination_of_columns')  AS COLUMN_REF,
	TEST_RESULTS_QUERY,
	STATUS,
	FAILURES,
	RESULT_ROWS,
	COALESCE(FAILED_ROW_COUNT, 0) as FAILED_ROW_COUNT

from {{ source('elementary', 'elementary_test_results') }}