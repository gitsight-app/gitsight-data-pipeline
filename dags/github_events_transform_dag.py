from enum import EnumType

import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, TaskGroup
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import datetime


class EventType(EnumType):
    WATCH = "WatchEvent"
    FORK = "ForkEvent"
    PULL_REQUEST = "PullRequestEvent"
    PUSH = "PushEvent"
    ISSUES = "IssuesEvent"


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
    schedule=CronDataIntervalTimetable("20 * * * *", timezone=pendulum.UTC),
    max_active_tasks=1,
    catchup=False,
    template_searchpath=["/opt/airflow/include"],
) as dag:
    start_task = EmptyOperator(task_id="start_task")

    with TaskGroup(group_id="transform_events") as transform_events_group:
        for event in target_events:
            task_id = f"transform_silver_{event['event_type']}_from_bronze"
            SparkKubernetesOperator(
                task_id=task_id,
                application_file="spark/jobs/transform_silver_events_from_bronze/application.yaml",
                namespace="spark-applications",
                params={
                    "event_type": event["event_type"],
                    "target_table": event["target_table"],
                },
            )

    end_events_transform = EmptyOperator(
        task_id="end_events_transform",
    )
    start_task >> transform_events_group >> end_events_transform
