import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, TriggerRule
from airflow.timetables.interval import CronDataIntervalTimetable

with DAG(
    dag_id="update_repo_metrics_daily",
    doc_md="""
    - Update repo metrics daily to calculate daily star and fork count, rank and trend.
    - This DAG is scheduled to run daily and will process the data for the previous day.
    """,
    start_date=pendulum.datetime(2026, 1, 1),
    schedule=CronDataIntervalTimetable("@daily", timezone=pendulum.UTC),
    template_searchpath=["/opt/airflow/include"],
    catchup=False,
) as dag:
    from operators.catalog.ref import NessieRefOperator, RefActionType

    create_nessie_branch = NessieRefOperator(
        task_id="create_nessie_branch",
        action=RefActionType.CREATE,
    )

    update_gold_repo_metrics_daily = SparkKubernetesOperator(
        task_id="update_gold_repo_metrics_daily",
        application_file="spark/jobs/update_repo_metrics_daily/application.yaml",
        namespace="spark-applications",
        params={
            "target_table_name": "nessie.gitsight.gold.repo_metrics_daily",
        },
    )

    gx_gold_repo_metrics_daily = SparkKubernetesOperator(
        task_id="gx_gold_repo_metrics_daily",
        application_file="spark/jobs/update_repo_metrics_daily/gx/application.yaml",
        namespace="spark-applications",
        params={
            "source_table_name": "nessie.gitsight.gold.repo_metrics_daily",
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

    load_oltp_gold_repo_metrics_hourly_to_staging = SparkKubernetesOperator(
        task_id="load_oltp_gold_repo_metrics_hourly_to_staging",
        application_file="spark/jobs/load_to_oltp_staging_daily/application.yaml",
        namespace="spark-applications",
        params={
            "source_table_name": "nessie.gitsight.gold.repo_metrics_daily",
            "target_table_name": "repo_metrics_daily_staging",
            "date_condition_col_name": "created_date",
            "use_main_ref": True,
        },
    )

    merge_staging_repo_metrics_to_prod = SQLExecuteQueryOperator(
        task_id="merge_staging_repo_metrics_to_prod",
        conn_id="postgres_default",
        sql="""
        INSERT INTO repo_metrics_daily (
                repo_id
                , star_count
                , fork_count
                , pr_count
                , issues_count
                , push_count
                , created_date
            )
        SELECT
            repo_id
            , star_count
            , fork_count
            , pr_count
            , issues_count
            , push_count
            , created_date
        FROM repo_metrics_daily_staging
        ON CONFLICT (repo_id, created_date)
        DO UPDATE SET
            repo_id = excluded.repo_id
            , star_count = excluded.star_count
            , fork_count = excluded.fork_count
            , pr_count = excluded.pr_count
            , issues_count = excluded.issues_count
            , push_count = excluded.push_count
        """,
        show_return_value_in_logs=True,
    )

    clear_staging_repo_metrics = SQLExecuteQueryOperator(
        task_id="clear_staging_repo_metrics",
        conn_id="postgres_default",
        sql="""DROP TABLE IF EXISTS repo_metrics_daily_staging
        """,  # noqa: E501
    )

    delete_nessie_branch = NessieRefOperator(
        task_id="delete_nessie_branch",
        action=RefActionType.DELETE,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        create_nessie_branch
        >> update_gold_repo_metrics_daily
        >> gx_gold_repo_metrics_daily
    )

    gx_gold_repo_metrics_daily >> merge_nessie_branch
    gx_gold_repo_metrics_daily >> skip_merge_nessie_branch

    merge_nessie_branch >> load_oltp_gold_repo_metrics_hourly_to_staging
    load_oltp_gold_repo_metrics_hourly_to_staging >> merge_staging_repo_metrics_to_prod
    merge_staging_repo_metrics_to_prod >> clear_staging_repo_metrics

    [clear_staging_repo_metrics, skip_merge_nessie_branch] >> delete_nessie_branch
