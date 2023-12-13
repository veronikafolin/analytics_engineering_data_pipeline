from airflow.models import TaskInstance
from airflow.utils.state import State


def branch_func_on_orders_freshness(**kwargs):
    execution_date = kwargs['execution_date']
    dag_instance = kwargs['dag']
    operator_instance = dag_instance.get_task("check_orders_freshness")
    task_status = TaskInstance(operator_instance, execution_date).current_state()
    if task_status == State.SUCCESS:
        return 'trigger_customer_dag'
    else:
        return 'dbt_run_stg_orders'
