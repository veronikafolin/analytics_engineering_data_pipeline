from airflow.utils.state import State
import common_utils


def branch_func_on_registry_stg_orders_initialization(**kwargs):
    task_status = common_utils.get_internal_task_state('check_registry_stg_orders_initialization', **kwargs)
    if task_status == State.SUCCESS:
        return 'dbt_run_registry_stg_orders'
    else:
        return 'dbt_run_registry_stg_orders_full_refresh'


def branch_func_on_registry_stg_lineitem_initialization(**kwargs):
    task_status = common_utils.get_internal_task_state('check_registry_stg_lineitem_initialization', **kwargs)
    if task_status == State.SUCCESS:
        return 'dbt_run_registry_stg_lineitem'
    else:
        return 'dbt_run_registry_stg_lineitem_full_refresh'
