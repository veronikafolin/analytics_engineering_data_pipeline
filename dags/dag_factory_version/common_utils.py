from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.models import TaskInstance, DagRun


def get_internal_task_state(task_id, **kwargs):
    execution_date = kwargs['execution_date']
    dag_instance = kwargs['dag']
    operator_instance = dag_instance.get_task(task_id)
    task_status = TaskInstance(operator_instance, execution_date).current_state()
    return task_status


def get_external_task_state(dag_id, task_id, **kwargs):
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    execution_date = dag_runs[0].execution_date
    operator_instance = get_task_instance(dag_id, task_id, execution_date)
    task_status = operator_instance.current_state()
    return task_status
