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


def _decide_merge(gx_task_ids, **context):
    dagrun = context["ti"].get_dagrun()
    states = [dagrun.get_task_instance(task_id).state for task_id in gx_task_ids]
    if all(state == "success" for state in states):
        return "merge_nessie_branch"
    return "skip_merge_nessie_branch"


with DAG(
    dag_id="repo_actor_master_transform",
    doc_md="""
    Loads the repository actor master data. to Silver layer.
    This DAG is scheduled to run daily and will process the data for the previous day.
    """,
    start_date=datetime(2026, 1, 1),
    schedule=CronDataIntervalTimetable("15 * * * *", timezone=pendulum.UTC),
    template_searchpath=["/opt/airflow/include"],
    catchup=False,
) as dag:
    application_base_path = "spark/jobs/repo_actor_master_transform"

    create_nessie_branch = NessieRefOperator(
        task_id="create_nessie_branch",
        action=RefActionType.CREATE,
    )

    with TaskGroup(
        group_id="elt_repo_master",
    ) as elt_repo_master:
        load_repo_master_to_silver = SparkKubernetesOperator(
            task_id="load_repo_master_to_silver",
            application_file=f"{application_base_path}/load_repo_master_to_silver/application.yaml",
            namespace="spark-applications",
        )

        gx_repo_master_to_silver = GXTableValidateOperator(
            task_id="gx_repo_master_to_silver",
            source_table_name="nessie.gitsight.silver.repo_master",
            date_column="ingested_at",
            identify_name="silver_repo_master",
        )

        load_repo_master_to_silver >> gx_repo_master_to_silver

    with TaskGroup(
        group_id="elt_actor_master",
    ) as elt_actor_master:
        load_actor_master_to_silver = SparkKubernetesOperator(
            task_id="load_actor_master_to_silver",
            application_file=f"{application_base_path}/load_actor_master_to_silver/application.yaml",
            namespace="spark-applications",
        )

        gx_actor_master_to_silver = GXTableValidateOperator(
            task_id="gx_actor_master_to_silver",
            source_table_name="nessie.gitsight.silver.actor_master",
            date_column="ingested_at",
            identify_name="silver_actor_master",
        )

        load_actor_master_to_silver >> gx_actor_master_to_silver

    before_to_handle_nessie_branch = EmptyOperator(
        task_id="before_to_handle_nessie_branch"
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

    create_nessie_branch >> [elt_actor_master, elt_repo_master]
    [elt_actor_master, elt_repo_master] >> before_to_handle_nessie_branch
    [
        gx_repo_master_to_silver,
        gx_actor_master_to_silver,
    ] >> before_to_handle_nessie_branch
    (
        before_to_handle_nessie_branch
        >> [merge_nessie_branch, skip_merge_nessie_branch]
        >> delete_nessie_branch
    )
