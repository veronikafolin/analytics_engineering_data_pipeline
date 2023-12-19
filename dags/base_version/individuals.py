from datetime import timedelta
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import dates
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule
from airflow_dbt.operators.dbt_operator import (
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtTestOperator,
)

import common_utils

default_args = {
    'start_date': dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(dag_id='dim_customer_1', default_args=default_args, schedule_interval=None):

    dbt_snapshot_customer = DbtSnapshotOperator(
        task_id='dbt_snapshot_customer',
        models='snapshot_customer'
    )

    check_customer_freshness = DbtTestOperator(
        task_id='check_customer_freshness',
        select='stg_customer,tag:freshness'
    )


    def branch_func_on_customer_freshness(**kwargs):
        task_status = common_utils.get_internal_task_state('check_customer_freshness', **kwargs)
        if task_status == State.SUCCESS:
            return 'trigger_nation_dag'
        else:
            return 'dbt_run_stg_customer'

    branch_on_customer_freshness = BranchPythonOperator(
        task_id='branch_on_customer_freshness',
        python_callable=branch_func_on_customer_freshness,
        provide_context=True
    )

    dbt_run_stg_customer = DbtRunOperator(
        task_id='dbt_run_stg_customer',
        models='stg_customer'
    )

    dbt_test_stg_customer = DbtTestOperator(
        task_id='dbt_test_stg_customer',
        models='stg_customer'
    )

    trigger_nation_dag = TriggerDagRunOperator(
        task_id="trigger_nation_dag",
        trigger_dag_id="int_nation_1",
        trigger_rule=TriggerRule.ALL_DONE,
        wait_for_completion=True
    )

    dbt_run_dim_customer = DbtRunOperator(
        task_id='dbt_run_dim_customer',
        models='dim_customer'
    )

    dbt_test_dim_customer = DbtTestOperator(
        task_id='dbt_test_dim_customer',
        models='dim_customer'
    )

    dbt_snapshot_customer >> check_customer_freshness >> branch_on_customer_freshness >> [trigger_nation_dag, dbt_run_stg_customer]
    dbt_run_stg_customer >> dbt_test_stg_customer >> trigger_nation_dag
    trigger_nation_dag >> dbt_run_dim_customer >> dbt_test_dim_customer


with DAG(dag_id='int_nation_1', default_args=default_args, schedule_interval=None):

    dbt_run_stg_nation = DbtRunOperator(
        task_id='dbt_run_stg_nation',
        models='stg_nation'
    )

    dbt_test_stg_nation = DbtTestOperator(
        task_id='dbt_test_stg_nation',
        models='stg_nation'
    )

    dbt_run_stg_region = DbtRunOperator(
        task_id='dbt_run_stg_region',
        models='stg_region'
    )

    dbt_test_stg_region = DbtTestOperator(
        task_id='dbt_test_stg_region',
        models='stg_region'
    )

    dbt_run_int_nation = DbtRunOperator(
        task_id='dbt_run_int_nation',
        models='int_nation'
    )

    dbt_test_int_nation = DbtTestOperator(
        task_id='dbt_test_int_nation',
        models='int_nation'
    )

    chain((dbt_run_stg_nation, dbt_run_stg_region), (dbt_test_stg_nation, dbt_test_stg_region), dbt_run_int_nation,
          dbt_test_int_nation)
