with

previous_state_of_test_results as (
    select *
    from {{this}}
),

new_state_of_test_results as (
    select *
    from {{ source('elementary', 'elementary_test_results') }}
),

final as (

    select
        new.TEST_UNIQUE_ID as TEST_UNIQUE_ID,
        new.MODEL_UNIQUE_ID as MODEL_UNIQUE_ID,
        new.DATABASE_NAME as DATABASE_NAME,
        new.SCHEMA_NAME as SCHEMA_NAME,
        new.TABLE_NAME as TABLE_NAME,
        NVL(new.COLUMN_NAME, 'combination_of_columns') as COLUMN_NAME,
        new.DATABASE_NAME || '.' || new.SCHEMA_NAME || '.' || new.TABLE_NAME  AS TABLE_REF,
        new.DATABASE_NAME || '.' || new.SCHEMA_NAME || '.' || new.TABLE_NAME || '.' || NVL(new.COLUMN_NAME, 'combination_of_columns')  AS COLUMN_REF,
        new.TEST_RESULTS_QUERY as TEST_RESULTS_QUERY,
        new.STATUS as STATUS,
        new.FAILURES as FAILURES,
        new.RESULT_ROWS as RESULT_ROWS,
        new.DETECTED_AT as DETECTED_AT,
        TO_DATE(new.DETECTED_AT) as DETECTED_AT_DATE,
        COALESCE(new.FAILED_ROW_COUNT, 0) as FAILED_ROW_COUNT,
        IFF(
            (COALESCE(new.FAILED_ROW_COUNT, 0) - COALESCE(old.FAILED_ROW_COUNT, 0)) = 0,
            COALESCE(new.FAILED_ROW_COUNT, 0),
            (COALESCE(new.FAILED_ROW_COUNT, 0) - COALESCE(old.FAILED_ROW_COUNT, 0))
        ) as FAILED_ROW_COUNT_DELTA
    from new_state_of_test_results as new left join previous_state_of_test_results as old on (COLUMN_REF = old.COLUMN_REF)

)

select * from final