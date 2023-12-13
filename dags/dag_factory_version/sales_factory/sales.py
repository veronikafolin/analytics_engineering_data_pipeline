from dagfactory import DagFactory
from airflow.models import TaskInstance
from airflow.utils.state import State


def branch_func_on_orders_freshness(**kwargs):
    execution_date = kwargs['execution_date']
    dag_instance = kwargs['dag']
    operator_instance = dag_instance.get_task("task_id")
    task_status = TaskInstance(operator_instance, execution_date).current_state()
    if task_status == State.SUCCESS:
        return 'trigger_customer_dag'
    else:
        return 'dbt_run_stg_orders'


dag_factory = DagFactory("/home/airflow/gcs/dags/dag_factory_version/sales_factory/config.yml")

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
