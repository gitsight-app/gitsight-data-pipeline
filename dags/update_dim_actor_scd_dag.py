import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, TriggerRule
from airflow.timetables.interval import CronDataIntervalTimetable
from operators.catalog.ref import NessieRefOperator, RefActionType
from pendulum import datetime

with DAG(
    dag_id="update_dim_actor_scd",
    schedule=CronDataIntervalTimetable("50 * * * *", timezone=pendulum.UTC),
    start_date=datetime(2026, 1, 1),
    max_active_tasks=1,
    catchup=False,
    template_searchpath=["/opt/airflow/include"],
) as dag:
    create_nessie_branch = NessieRefOperator(
        task_id="create_nessie_branch",
        action=RefActionType.CREATE,
    )

    fetch_actor_detail_to_raw = SparkKubernetesOperator(
        task_id="fetch_actor_detail_to_raw",
        application_file="spark/jobs/update_dim_actor_scd/fetch_actor_detail_to_raw/application.yaml",
        namespace="spark-applications",
    )

    merge_dim_actor_scd = SparkKubernetesOperator(
        task_id="merge_dim_actor_scd",
        application_file="spark/jobs/update_dim_actor_scd/merge_dim_actor_scd/application.yaml",
        namespace="spark-applications",
    )

    gx_actor_detail_scd = SparkKubernetesOperator(
        task_id="gx_actor_detail_scd",
        application_file="spark/jobs/gx_table_validation/application.yaml",
        namespace="spark-applications",
        params={
            "source_table_name": "nessie.gitsight.silver.actor_detail_scd",
            "date_column": "ingested_at",
            "data_interval_start": "{{ data_interval_start }}",
            "data_interval_end": "{{ data_interval_end }}",
            "suite_name": "silver_actor_detail_scd_suite",
            "checkpoint_name": "silver_actor_detail_scd_checkpoint",
            "data_asset_name": "silver_actor_detail_scd",
        },
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
        >> fetch_actor_detail_to_raw
        >> merge_dim_actor_scd
        >> gx_actor_detail_scd
    )

    gx_actor_detail_scd >> merge_nessie_branch
    gx_actor_detail_scd >> skip_merge_nessie_branch

    merge_nessie_branch >> delete_nessie_branch
    skip_merge_nessie_branch >> delete_nessie_branch
