from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from datetime import datetime, timedelta

import os

DBT_PROJECT_PATH = f"{os.environ["AIRFLOW_HOME"]}/dags/dbt/astro-etl-dbt/"
DBT_EXECUTABLE_PATH = f"{os.environ["AIRFLOW_HOME"]}/dbt_venv/bin/dbt"

profile_config_stage = ProfileConfig(
    profile_name="astro-etl-dbt",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id = "postgres_default",
        profile_args={"schema": "sales_stage"}
    )
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH
)

default_args = {
    "owner":"airflow",
    "start_date":datetime(2024,10,23),
    "email_on_failure":False,
    "email_on_retry":False,
    "retries":3,
    "retry_delay":timedelta(minutes=1)
}

with DAG(
    dag_id="dag_sales_stage",
    default_args=default_args,
    description="Executa run do dbt para os modelos stage indicados.",
    schedule_interval=None,
    catchup=False,
    tags=["sales","stage"]
):
    start = EmptyOperator(task_id = "begin")

    dbt_run_and_test = DbtTaskGroup(
        group_id="execute_sales_stage_layer",
        project_config=ProjectConfig(DBT_PROJECT_PATH,
                                     models_relative_path="models"),
        profile_config=profile_config_stage,
        execution_config=execution_config,
        render_config=RenderConfig(select=["path:models/sales_staging"]),
        operator_args={
            "install_deps": False
        },
        default_args={"retries": 1,
                      "retry_delay":timedelta(seconds=15)}
    )

    start_other_dag = TriggerDagRunOperator(
        task_id="start_trusted_dag",
        trigger_dag_id='dag_sales_trusted'
    )

    start >> dbt_run_and_test >> start_other_dag