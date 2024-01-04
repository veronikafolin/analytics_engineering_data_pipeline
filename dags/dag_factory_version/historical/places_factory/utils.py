from airflow.utils.state import State
import common_utils


def branch_func_on_int_nation_existence(**kwargs):
    task_status = common_utils.get_internal_task_state('check_int_nation_existence', **kwargs)
    if task_status == State.SUCCESS:
        return None
    else:
        return ['dbt_run_stg_region']
