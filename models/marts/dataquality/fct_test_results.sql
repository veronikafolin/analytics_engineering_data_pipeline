with

tests as (
    select * from {{ref('stg_dbt_tests')}}
),

test_results as (
    select * from {{ref('stg_elementary_test_results')}}
),

final as (
    select
        ID as TEST_RESULT_ID,
        TEST_UNIQUE_ID,
        TEST_NAME,
        TEST_SHORT_NAME,
        TEST_ALIAS,
        DETECTED_AT,
        MODEL_UNIQUE_ID,
        test_results.DATABASE_NAME,
        test_results.SCHEMA_NAME,
        TABLE_NAME,
        COLUMN_NAME,
        OWNERS,
        test_results.TAGS,
        MODEL_OWNERS,
        MODEL_TAGS,
        test_results.TEST_PARAMS,
        META,
        DESCRIPTION,
	    ORIGINAL_PATH,
	    GENERATED_AT,
	    METADATA_HASH,
        TEST_RESULTS_QUERY,
        test_results.SEVERITY,
        STATUS,
        FAILURES,
        RESULT_ROWS,
        FAILED_ROW_COUNT
    from test_results
    join tests on (test_results.TEST_UNIQUE_ID = tests.UNIQUE_ID)
)

select * from final