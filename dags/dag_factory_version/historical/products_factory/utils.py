from airflow.utils.state import State
import common_utils


def branch_func_on_registry_stg_part_initialization(**kwargs):
    task_status = common_utils.get_internal_task_state('check_registry_stg_part_initialization', **kwargs)
    if task_status == State.SUCCESS:
        return 'dbt_run_registry_stg_part'
    else:
        return 'dbt_run_registry_stg_part_full_refresh'


def branch_func_on_registry_stg_partsupp_initialization(**kwargs):
    task_status = common_utils.get_internal_task_state('check_registry_stg_partsupp_initialization', **kwargs)
    if task_status == State.SUCCESS:
        return 'dbt_run_registry_stg_partsupp'
    else:
        return 'dbt_run_registry_stg_partsupp_full_refresh'
