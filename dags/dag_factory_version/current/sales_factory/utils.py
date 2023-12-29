from airflow.utils.state import State
import common_utils


def branch_func_on_orders_freshness(**kwargs):
    task_status = common_utils.get_internal_task_state('check_orders_freshness', **kwargs)
    if task_status == State.SUCCESS:
        return 'trigger_customer_dag'
    else:
        return 'dbt_run_stg_orders'


def branch_func_on_dbt_test_dim_customer_state(**kwargs):
    task_status = common_utils.get_external_task_state('dim_customer_2', 'dbt_test_dim_customer', **kwargs)
    if task_status == State.SUCCESS:
        return 'dbt_run_fct_orders'
    else:
        return None
