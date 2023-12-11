from datetime import timedelta
import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtRunOperator,
    DbtTestOperator,
)

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(dag_id='monitor_dataquality_1', default_args=default_args, schedule_interval=None):
    dbt_seed = DbtSeedOperator(
        task_id='dbt_seed',
        select='test_tags'
    )

    dbt_run_stg_dbt_tests = DbtRunOperator(
        task_id='dbt_run_stg_dbt_tests',
        models='stg_dbt_tests'
    )

    dbt_run_stg_elementary_test_results = DbtRunOperator(
        task_id='dbt_run_stg_elementary_test_results',
        models='stg_elementary_test_results'
    )

    dbt_run_fct_test_results = DbtRunOperator(
        task_id='dbt_run_fct_test_results',
        models='fct_test_results'
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
        models='monitor_dataquality',
        vars={'groupBy': get_groupby(), 'filters': get_filters()}
    )

    (dbt_seed, dbt_run_stg_dbt_tests, dbt_run_stg_elementary_test_results) >> dbt_run_fct_test_results >> dbt_run_dashboard


with DAG(dag_id='elementary_1', default_args=default_args, schedule_interval=None):
    dbt_test = DbtTestOperator(
        task_id='dbt_test',
    )

    report = BashOperator(
        task_id="edr_report",
        bash_command="edr report",
    )

    monitor = BashOperator(
        task_id="edr_monitor",
        bash_command="edr monitor",
    )

    dbt_test >> report >> monitor
