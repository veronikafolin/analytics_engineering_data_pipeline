from airflow.utils.state import State
import common_utils


def branch_func_on_customer_freshness(**kwargs):
    task_status = common_utils.get_internal_task_state('check_customer_freshness', **kwargs)
    if task_status == State.SUCCESS:
        return 'trigger_nation_dag'
    else:
        return 'dbt_run_stg_customer'
