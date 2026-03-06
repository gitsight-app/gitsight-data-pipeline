from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from operators.common.code_deploy import CodeDeployOperator
from operators.spark.TransformSilverEventOperator import (
    EventType,
    TransformSilverEventOperator,
)
from pendulum import datetime

with DAG(
    dag_id="github_events_transform",
    doc_md="""
    Transform github events data in silver layer from bronze gharchive events table
    - Watch Events(Star Events)

    """,
    start_date=datetime(2026, 1, 1),
    schedule="20 * * * *",
    catchup=False,
) as dag:
    deploy_spark_code = CodeDeployOperator(
        task_id="deploy_spark_code",
        folder_path="/opt/airflow/include",
        s3_bucket="gitsight",
        s3_key="artifacts/builds/dev/include.zip",
        aws_conn_id="aws_default",
    )

    transform_silver_watch_events_from_bronze = TransformSilverEventOperator(
        task_id="transform_silver_watch_events_from_bronze",
        py_files="{{ ti.xcom_pull(task_ids='deploy_spark_code') }}",
        executor_memory="1g",
        aws_conn_id="aws_default",
        event_type=EventType.WATCH,
        target_table="nessie.gitsight.silver.watch_events",
        catalog_conn_id="catalog_default",
        verbose=True,
    )

    transform_silver_fork_events_from_bronze = TransformSilverEventOperator(
        task_id="transform_silver_fork_events_from_bronze",
        py_files="{{ ti.xcom_pull(task_ids='deploy_spark_code') }}",
        executor_memory="1g",
        aws_conn_id="aws_default",
        event_type=EventType.FORK,
        target_table="nessie.gitsight.silver.fork_events",
        catalog_conn_id="catalog_default",
        verbose=True,
    )

    end_events_transform = EmptyOperator(
        task_id="end_events_transform",
    )
    (
        deploy_spark_code
        >> [
            transform_silver_watch_events_from_bronze,
            transform_silver_fork_events_from_bronze,
        ]
        >> end_events_transform
    )
