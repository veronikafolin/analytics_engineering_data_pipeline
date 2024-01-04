from airflow.utils.state import State
import common_utils


def branch_func_customer_on_int_nation_existence(**kwargs):
    task_status = common_utils.get_internal_task_state('check_int_nation_existence', **kwargs)
    if task_status == State.SUCCESS:
        return 'dbt_test_registry_stg_customer'
    else:
        return 'trigger_nation_dag'


def branch_func_supplier_on_int_nation_existence(**kwargs):
    task_status = common_utils.get_internal_task_state('check_int_nation_existence', **kwargs)
    if task_status == State.SUCCESS:
        return 'dbt_test_registry_stg_supplier'
    else:
        return 'trigger_nation_dag'


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
