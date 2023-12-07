from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow_dbt.operators.dbt_operator import (
    DbtDepsOperator
)

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='setup_dbt_project', default_args=default_args, schedule_interval=None):

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command="dbt debug",
    )

    dbt_deps = DbtDepsOperator(
        task_id='dbt_deps',
    )

    dbt_debug >> dbt_deps
