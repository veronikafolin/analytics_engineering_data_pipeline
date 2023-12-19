from datetime import timedelta

from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
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

with (DAG(dag_id='fct_orders_1', default_args=default_args, schedule_interval=None)):

    dbt_snapshot_orders = DbtSnapshotOperator(
        task_id='dbt_snapshot_orders',
        models='snapshot_orders'
    )

    check_orders_freshness = DbtTestOperator(
        task_id='check_orders_freshness',
        select='stg_orders, tag:freshness'
    )

    def branch_func_on_orders_freshness(**kwargs):
        task_status = common_utils.get_internal_task_state('check_orders_freshness', **kwargs)
        if task_status == State.SUCCESS:
            return 'trigger_customer_dag'
        else:
            return 'dbt_run_stg_orders'


    branch_on_orders_freshness = BranchPythonOperator(
        task_id='branch_on_orders_freshness',
        python_callable=branch_func_on_orders_freshness,
        provide_context=True
    )

    dbt_run_stg_orders = DbtRunOperator(
        task_id='dbt_run_stg_orders',
        models='stg_orders'
    )

    dbt_test_stg_orders = DbtTestOperator(
        task_id='dbt_test_stg_orders',
        models='stg_orders'
    )

    trigger_customer_dag = TriggerDagRunOperator(
        task_id="trigger_customer_dag",
        trigger_dag_id="dim_customer_1",
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_DONE
    )

    customer_update_sensor = ExternalTaskSensor(
        task_id="customer_update_sensor",
        external_dag_id="dim_customer_1",
        external_task_id="dbt_test_dim_customer",
        check_existence=True,
        timeout=60*10,  # it will fail after 10 minutes
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_date_fn=common_utils.get_execution_date_of('dim_customer_1'),
        trigger_rule=TriggerRule.ALL_DONE
    )

    dbt_run_fct_orders = DbtRunOperator(
        task_id='dbt_run_fct_orders',
        models='fct_orders'
    )

    dbt_test_fct_orders = DbtTestOperator(
        task_id='dbt_test_fct_orders',
        models='fct_orders'
    )

    dbt_snapshot_orders >> check_orders_freshness >> branch_on_orders_freshness >> [trigger_customer_dag,
                                                                                    dbt_run_stg_orders]
    dbt_run_stg_orders >> dbt_test_stg_orders >> trigger_customer_dag
    trigger_customer_dag >> customer_update_sensor >> dbt_run_fct_orders
    dbt_run_fct_orders >> dbt_test_fct_orders
