from datetime import timedelta

from airflow import DAG

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import dates
from airflow_dbt.operators.dbt_operator import (
    DbtRunOperator,
)

import common_utils

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

    dbt_run_dashboard = DbtRunOperator(
        task_id='dbt_run_dashboard',
        models='volume_orders',
        vars={'groupBy': common_utils.get_groupby(), 'filters': common_utils.get_filters()}
    )

    trigger_build_fct_orders >> dbt_run_dashboard
