from enum import EnumType

import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, TaskGroup, TriggerRule
from airflow.timetables.interval import CronDataIntervalTimetable
from operators.catalog.ref import NessieRefOperator, RefActionType
from operators.gx.table_validator import GXTableValidateOperator
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
    create_nessie_branch = NessieRefOperator(
        task_id="create_nessie_branch", action=RefActionType.CREATE
    )

    with TaskGroup(group_id="transform_events") as elt_transform_group:
        for event in target_events:
            task_id = f"transform_silver_{event['event_type']}_from_bronze"
            gx_task_id = f"gx_validate_silver_{event['event_type']}"
            elt_task = SparkKubernetesOperator(
                task_id=task_id,
                application_file="spark/jobs/transform_silver_events_from_bronze/application.yaml",
                namespace="spark-applications",
                params={
                    "event_type": event["event_type"],
                    "target_table": event["target_table"],
                },
            )

            gx_task = GXTableValidateOperator(
                task_id=gx_task_id,
                source_table_name=event["target_table"],
                date_column="ingested_at",
                identify_name=f"silver_{event['event_type']}",
            )

            elt_task >> gx_task

    merge_nessie_branch = NessieRefOperator(
        task_id="merge_nessie_branch",
        action=RefActionType.MERGE,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    skip_merge_nessie_branch = EmptyOperator(
        task_id="skip_merge_nessie_branch",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    delete_nessie_branch = NessieRefOperator(
        task_id="delete_nessie_branch",
        action=RefActionType.DELETE,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_nessie_branch >> elt_transform_group

    elt_transform_group >> merge_nessie_branch
    elt_transform_group >> skip_merge_nessie_branch

    [merge_nessie_branch, skip_merge_nessie_branch] >> delete_nessie_branch
