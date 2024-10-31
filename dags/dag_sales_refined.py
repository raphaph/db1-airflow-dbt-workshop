from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig, RenderConfig

import os
from datetime import datetime, timedelta 

DBT_PROJECT_PATH = f"{os.environ["AIRFLOW_HOME"]}/dags/dbt/astro_etl_dbt"
DBT_EXECUTABLE_PATH = f"{os.environ["AIRFLOW_HOME"]}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="astro_etl_dbt",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={"schema":"sales_refined"}
    )
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH
)

default_args = {
    "owner":"airflow",
    "start_date":datetime(2024,10,24),
    "email_on_failure":False,
    "email_on_retry":False,
    "retries":1,
    "retry_delay":timedelta(seconds=15)
}

def dbtRunAndTest(model):
    return DbtTaskGroup(
        group_id=f"execute_{model}",
        project_config=ProjectConfig(
            DBT_PROJECT_PATH,
            models_relative_path="models"
        ),
        profile_config=profile_config,
        render_config=RenderConfig(select=[
            f"path:models/sales_refined/{model}.sql"]),
        operator_args={"install_deps": False},
        default_args={"retries": 1,
                      "retry_delay": timedelta(seconds=30)}
    )

with DAG(
    dag_id="dag_sales_refined",
    default_args=default_args,
    description="Executa camada refined para sales",
    schedule_interval=None,
    catchup=False,
    tags=["refined","sales"]
):
    start = EmptyOperator(task_id = "Start")

    run_fat = dbtRunAndTest("fat_customer_orders_day")
    run_agr = dbtRunAndTest("agr_customer_orders_mon")
    
    start >> run_fat >> run_agr