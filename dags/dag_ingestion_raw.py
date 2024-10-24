from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta, datetime
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

s3_file_path = "s3://astro-learning/"
s3_conn_id = "aws_default"
postgres_conn_id = "postgres_default"

default_args = {
    "owner":"airflow",
    "start_date":datetime(2024,10,15),
    "email_on_failure":False,
    "email_on_retry":False,
    "retries":3,
    "retry_delay":timedelta(minutes=5)
}

with DAG(
    dag_id="dag_ingestion_raw",
    default_args=default_args,
    description="Executa extracao do s3 para o postgres",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["s3","postgres"]
):

    customers_data = aql.load_file(
        task_id="extract_customers_from_s3",
        input_file=File(
            path=s3_file_path + "customers.csv", conn_id=s3_conn_id
        ),
        output_table=Table(
            name="tb_customers", 
            conn_id=postgres_conn_id,
            metadata=Metadata(schema="raw_data")
        ),
        if_exists="replace"
    )

    orders_data = aql.load_file(
        task_id="extract_orders_from_s3",
        input_file=File(
            path=s3_file_path + "orders.csv", conn_id=s3_conn_id
        ),
        output_table=Table(
            name="tb_orders", 
            conn_id=postgres_conn_id,
            metadata=Metadata(schema="raw_data")
        ),
        if_exists="replace"
    )

    payments_data = aql.load_file(
        task_id="extract_payments_from_s3",
        input_file=File(
            path=s3_file_path + "order_payments.csv", conn_id=s3_conn_id
        ),
        output_table=Table(
            name="tb_order_payments", 
            conn_id=postgres_conn_id,
            metadata=Metadata(schema="raw_data")
        ),
        if_exists="replace"
    )

    items_data = aql.load_file(
        task_id="extract_items_from_s3",
        input_file=File(
            path=s3_file_path + "order_items.csv", conn_id=s3_conn_id
        ),
        output_table=Table(
            name="tb_order_items", 
            conn_id=postgres_conn_id,
            metadata=Metadata(schema="raw_data")
        ),
        if_exists="replace"
    )

    start_other_dag = TriggerDagRunOperator(
        task_id="start_stage_dag",
        trigger_dag_id='dag_sales_stage'
    )

    customers_data >> orders_data >> payments_data >> items_data >> start_other_dag