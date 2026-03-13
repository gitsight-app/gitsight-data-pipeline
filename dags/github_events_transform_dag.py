from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, TaskGroup
from operators.common.code_deploy import CodeDeployOperator
from operators.spark.transform_silver_event import (
    EventType,
    TransformSilverEventOperator,
)
from pendulum import datetime

target_events = [
    {
        "event_type": EventType.WATCH,
        "target_table": "nessie.gitsight.silver.watch_events",
    },
    {
        "event_type": EventType.FORK,
        "target_table": "nessie.gitsight.silver.fork_events",
    },
    {
        "event_type": EventType.PULL_REQUEST,
        "target_table": "nessie.gitsight.silver.pull_request_events",
    },
    {
        "event_type": EventType.PUSH,
        "target_table": "nessie.gitsight.silver.push_events",
    },
    {
        "event_type": EventType.ISSUES,
        "target_table": "nessie.gitsight.silver.issues_events",
    },
]

with DAG(
    dag_id="github_events_transform",
    doc_md="""
    Transform github events data in silver layer from bronze gharchive events table
    """,
    start_date=datetime(2026, 1, 1),
    schedule="20 * * * *",
    max_active_tasks=1,
    catchup=False,
) as dag:
    deploy_spark_code = CodeDeployOperator(
        task_id="deploy_spark_code",
        folder_path="/opt/airflow/include",
        s3_bucket="gitsight",
        s3_key="artifacts/builds/dev/include.zip",
        aws_conn_id="aws_default",
    )

    with TaskGroup(group_id="transform_events") as transform_events:
        for event in target_events:
            task_id = f"transform_silver_{event['event_type']}_from_bronze"
            TransformSilverEventOperator(
                task_id=task_id,
                py_files="{{ ti.xcom_pull(task_ids='deploy_spark_code') }}",
                executor_memory="1g",
                aws_conn_id="aws_default",
                event_type=event["event_type"],
                target_table=event["target_table"],
                catalog_conn_id="catalog_default",
                verbose=True,
            )

    end_events_transform = EmptyOperator(
        task_id="end_events_transform",
    )
    deploy_spark_code >> transform_events >> end_events_transform
