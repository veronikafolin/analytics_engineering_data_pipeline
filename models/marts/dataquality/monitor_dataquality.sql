with

test_results as (
    select * from {{ref('fct_test_results')}}
),

final as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        count(distinct TEST_UNIQUE_ID) as test_count,
        count(TEST_SHORT_NAME) as test_runs,
        sum(FAILURES) as failures_count,
        sum(ROW_COUNT) as row_count,
        sum(FAILED_ROW_COUNT) as failed_row_count,
        count(distinct COLUMN_REF) as points_of_control,
        CAST(avg(1 - (FAILED_ROW_COUNT/ROW_COUNT)) AS DECIMAL(10,2)) as quality_score
    from test_results
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
)

select * from final