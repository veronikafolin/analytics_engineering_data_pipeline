from airflow.utils.state import State
import common_utils


def branch_func_on_registry_stg_customer_initialization(**kwargs):
    task_status = common_utils.get_internal_task_state('check_registry_stg_customer_initialization', **kwargs)
    if task_status == State.SUCCESS:
        return 'dbt_run_registry_stg_customer'
    else:
        return 'dbt_run_registry_stg_customer_full_refresh'


def branch_func_on_registry_stg_supplier_initialization(**kwargs):
    task_status = common_utils.get_internal_task_state('check_registry_stg_supplier_initialization', **kwargs)
    if task_status == State.SUCCESS:
        return 'dbt_run_registry_stg_supplier'
    else:
        return 'dbt_run_registry_stg_supplier_full_refresh'
