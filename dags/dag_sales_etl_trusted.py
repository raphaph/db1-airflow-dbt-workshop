from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from datetime import datetime, timedelta

import os

DBT_PROJECT_PATH = f"{os.environ["AIRFLOW_HOME"]}/dags/dbt/astro-etl-dbt/"
DBT_EXECUTABLE_PATH = f"{os.environ["AIRFLOW_HOME"]}/dbt_venv/bin/dbt"

profile_config_trusted = ProfileConfig(
    profile_name="astro-etl-dbt",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id = "postgres_default",
        profile_args={"schema": "sales_trusted"}
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
    "retry_delay":timedelta(minutes=5)
}

with DAG(
    dag_id="dag_sales_trusted",
    default_args=default_args,
    description="Executa run do dbt para os modelos stage indicados.",
    schedule_interval=None,
    catchup=False,
    tags=["sales","sales_trusted"]
):
    start = EmptyOperator(task_id = "start")

    dbt_run_and_test = DbtTaskGroup(
        group_id="execute_sales_trusted_layer",
        project_config=ProjectConfig(DBT_PROJECT_PATH,
                                     models_relative_path="models"),
        profile_config=profile_config_trusted,
        execution_config=execution_config,
        render_config=RenderConfig(select=["path:models/sales_trusted"]),
        operator_args={
            "install_deps": False
        },
        default_args={"retries": 2}
    )

    start >> dbt_run_and_test