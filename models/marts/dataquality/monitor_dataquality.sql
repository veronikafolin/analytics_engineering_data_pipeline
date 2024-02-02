with

test_results as (
    select * from {{ref('fct_test_results')}}
    where partition_date = (select MAX(partition_date) from {{ref('fct_test_results')}})
),

final as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        count(distinct TEST_UNIQUE_ID) as test_count,
        count(TEST_SHORT_NAME) as test_runs,
        sum(FAILURES) as failures_count,
        sum(ROW_COUNT) as row_count,
        sum(ROW_COUNT_DELTA) as row_count_delta,
        sum(FAILED_ROW_COUNT) as failed_row_count,
        sum(FAILED_ROW_COUNT_DELTA) as failed_row_count_delta,
        count(distinct COLUMN_REF) as points_of_control,
        CAST(avg(1 - (FAILED_ROW_COUNT_DELTA/ROW_COUNT_DELTA)) AS DECIMAL(10,5)) as quality_score
    from test_results
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
)

select * from final