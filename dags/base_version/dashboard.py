from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import dates
from airflow_dbt.operators.dbt_operator import (
    DbtRunOperator,
)

default_args = {
    'start_date': dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(dag_id='volume_orders_1', default_args=default_args, schedule_interval=None):

    trigger_build_fct_orders = TriggerDagRunOperator(
        task_id="trigger_build_fct_orders",
        trigger_dag_id="fct_orders_1",
        wait_for_completion=True
    )

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


    dbt_run_dashboard = DbtRunOperator(
        task_id='dbt_run_dashboard',
        models='volume_orders',
        vars={'groupBy': get_groupby(), 'filters': get_filters()}
    )

    trigger_build_fct_orders >> dbt_run_dashboard
