from airflow.utils.state import State
import common_utils


def branch_func_on_metadata_initialization(**kwargs):
    task_status = common_utils.get_internal_task_state('check_metadata_initialization', **kwargs)
    if task_status == State.SUCCESS:
        return 'dbt_run_metadata'
    else:
        return 'dbt_run_metadata_full_refresh'


def branch_func_on_stg_elementary_test_results_initialization(**kwargs):
    task_status = common_utils.get_internal_task_state('check_stg_elementary_test_results_initialization', **kwargs)
    if task_status == State.SUCCESS:
        return 'dbt_run_stg_elementary_test_results'
    else:
        return 'dbt_run_stg_elementary_test_results_full_refresh'


def branch_func_on_fct_test_results_initialization(**kwargs):
    task_status = common_utils.get_internal_task_state('check_fct_test_results_initialization', **kwargs)
    if task_status == State.SUCCESS:
        return 'dbt_run_fct_test_results'
    else:
        return 'dbt_run_fct_test_results_full_refresh'
