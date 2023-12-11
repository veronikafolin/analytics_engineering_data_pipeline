with

test_results as (
    select * from {{ref('fct_test_results')}}
),

final as (
    select
        {{ write_select_groupByColumns_by_vars() }}
        count(distinct TEST_UNIQUE_ID) as distinct_number_of_tests,
        count(TEST_SHORT_NAME) as number_of_test_runs,
        sum(FAILURES) as sum_of_failures,
        sum(FAILED_ROW_COUNT) as sum_of_failed_row_count
    from test_results
    {{ write_where_by_vars() }}
    {{ write_groupBY_groupByColumns_by_vars() }}
)

select * from final