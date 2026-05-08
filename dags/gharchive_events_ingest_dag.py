import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TriggerRule
from airflow.timetables.interval import CronDataIntervalTimetable
from hook.gh_archive import GHArchiveHook
from operators.catalog.ref import NessieRefOperator, RefActionType
from operators.gx.table_validator import GXTableValidateOperator


def _save_gharchive_to_s3(*, aws_conn_id: str, bucket_name: str, **context):
    hook = GHArchiveHook(aws_conn_id=aws_conn_id, bucket_name=bucket_name)
    date: pendulum.datetime = context["data_interval_start"]
    return hook.save_archive(date)


with DAG(
    dag_id="gharchive_events_ingest",
    doc_md="""
    - Ingest Gharchive data from https://www.gharchive.org/
    - Extract actor and repo meta from gharchive events in bronze layer
    """,
    start_date=pendulum.datetime(2026, 1, 1),
    schedule=CronDataIntervalTimetable("10 * * * *", timezone=pendulum.UTC),
    catchup=False,
    template_searchpath=["/opt/airflow/include"],
) as dag:
    spark_job_base_path = "spark/jobs/gharchive_events_ingest"

    create_nessie_branch = NessieRefOperator(
        task_id="create_nessie_branch", action=RefActionType.CREATE
    )

    save_gharchive_to_s3 = PythonOperator(
        task_id="save_gharchive_to_s3",
        python_callable=_save_gharchive_to_s3,
        op_kwargs={
            "aws_conn_id": "aws_default",
            "bucket_name": "gitsight",
        },
    )

    extract_gharchive_events_to_bronze = SparkKubernetesOperator(
        task_id="extract_gharchive_events_to_bronze",
        application_file=f"{spark_job_base_path}/extract_gharchive_events_to_bronze/application.yaml",
        namespace="spark-applications",
    )

    gx_extract_gharchive_events_to_bronze = SparkKubernetesOperator(
        task_id="gx_extract_gharchive_events_to_bronze",
        application_file=f"{spark_job_base_path}/extract_gharchive_events_to_bronze/gx/application.yaml",
        namespace="spark-applications",
    )

    extract_actor_meta_from_bronze_events = SparkKubernetesOperator(
        task_id="extract_actor_meta_from_bronze_events",
        application_file=f"{spark_job_base_path}/extract_actor_meta_from_bronze_events/application.yaml",
        namespace="spark-applications",
    )

    gx_actor_meta_from_bronze_events = GXTableValidateOperator(
        task_id="gx_actor_meta_from_bronze_events",
        source_table_name="nessie.gitsight.bronze.repo_meta",
        date_column="ingested_at",
        identify_name="bronze_actor_meta",
    )

    extract_repo_meta_from_bronze_events = SparkKubernetesOperator(
        task_id="extract_repo_meta_from_bronze_events",
        application_file=f"{spark_job_base_path}/extract_repo_meta_from_bronze_events/application.yaml",
        namespace="spark-applications",
    )

    gx_repo_meta_from_bronze_events = GXTableValidateOperator(
        task_id="gx_repo_meta_from_bronze_events",
        source_table_name="nessie.gitsight.bronze.repo_meta",
        date_column="ingested_at",
        identify_name="bronze_repo_meta",
    )

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

    (
        create_nessie_branch
        >> save_gharchive_to_s3
        >> extract_gharchive_events_to_bronze
        >> gx_extract_gharchive_events_to_bronze
    )

    (
        gx_extract_gharchive_events_to_bronze
        >> extract_actor_meta_from_bronze_events
        >> gx_actor_meta_from_bronze_events
    )
    (
        gx_extract_gharchive_events_to_bronze
        >> extract_repo_meta_from_bronze_events
        >> gx_repo_meta_from_bronze_events
    )

    [
        gx_actor_meta_from_bronze_events,
        gx_repo_meta_from_bronze_events,
    ] >> merge_nessie_branch
    (
        [
            gx_actor_meta_from_bronze_events,
            gx_repo_meta_from_bronze_events,
        ]
        >> skip_merge_nessie_branch
    )

    [merge_nessie_branch, skip_merge_nessie_branch] >> delete_nessie_branch
