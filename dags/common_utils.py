from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.models import TaskInstance, DagRun
from airflow.decorators import task


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


@task
def get_groupby(**context):
    groupBy = context['dag_run'].conf.get('groupBy')
    if groupBy is None:
        return []
    else:
        return groupBy


@task
def get_filters(**context):
    filters = context['dag_run'].conf.get('filters')
    if filters is None:
        return []
    else:
        return filters


def get_execution_date_of(dag_id):
    def get_last_execution_date(exec_date, **kwargs):
        dag_runs = DagRun.find(dag_id=dag_id)
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
        return dag_runs[0].execution_date if dag_runs else None

    return get_last_execution_date
