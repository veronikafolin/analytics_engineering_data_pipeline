from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow_dbt.operators.dbt_operator import (
    DbtDocsGenerateOperator
)

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='deploy_dbt_project_1', default_args=default_args, schedule_interval=None):

    dbt_docs_generate = DbtDocsGenerateOperator(
        task_id='dbt_docs',
    )

    dbt_docs_serve = BashOperator(
        task_id="dbt_docs_serve",
        bash_command="dbt docs serve",
    )

    dbt_docs_generate >> dbt_docs_serve
