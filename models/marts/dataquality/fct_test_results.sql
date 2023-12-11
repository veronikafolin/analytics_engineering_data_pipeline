with

tests as (
    select * from {{ref('stg_dbt_tests')}}
),

test_results as (
    select * from {{ref('stg_elementary_test_results')}}
),

test_tags as (
    select * from {{ref('test_tags')}}
),

final as (
    select
        test_results.TEST_UNIQUE_ID,
        tests.TEST_SHORT_NAME,
        test_results.MODEL_UNIQUE_ID,
        test_results.DATABASE_NAME,
        test_results.SCHEMA_NAME,
        test_results.TABLE_NAME,
        test_results.COLUMN_NAME,
        tests.TEST_OWNERS,
        test_tags.TEST_TAG,
        tests.MODEL_OWNERS,
        tests.MODEL_TAGS,
        tests.DESCRIPTION,
	    tests.ORIGINAL_PATH,
	    tests.GENERATED_AT,
        test_results.TEST_RESULTS_QUERY,
        test_results.STATUS,
        test_results.FAILURES,
        test_results.RESULT_ROWS,
        test_results.FAILED_ROW_COUNT
    from test_results
    join tests on (test_results.TEST_UNIQUE_ID = tests.UNIQUE_ID)
    join test_tags on (tests.TEST_SHORT_NAME = test_tags.TEST_NAME)
)

select * from final