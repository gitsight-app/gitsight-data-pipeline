import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import datetime

with DAG(
    dag_id="update_repo_contribution_metrics_daily",
    start_date=datetime(2026, 1, 1),
    schedule=CronDataIntervalTimetable("10 0 * * *", timezone=pendulum.UTC),
    catchup=False,
    template_searchpath=["/opt/airflow/include"],
) as dag:
    py_file_xcom = "{{ ti.xcom_pull(task_ids='deploy_spark_code') }}"

    update_gold_repo_metrics = SparkKubernetesOperator(
        task_id="update_gold_repo_metrics",
        application_file="spark/jobs/update_repo_contribution_metrics_daily/application.yaml",
        namespace="spark-applications",
    )

    staging_gold_repo_metrics = SparkKubernetesOperator(
        task_id="staging_gold_repo_metrics",
        application_file="spark/jobs/load_to_oltp_staging_daily/application.yaml",
        params={
            "source_table_name": "nessie.gitsight.gold.repo_contribution_metrics_daily",
            "staging_table_name": "repo_contribution_metrics_daily_staging",
            "date_condition_col_name": "created_date",
        },
    )

    merge_staging_repo_metrics_to_prod = SQLExecuteQueryOperator(
        task_id="merge_staging_repo_metrics_to_prod",
        conn_id="postgres_default",
        sql="""
        INSERT INTO repo_contribution_metrics_daily (
                repo_id
                , country_code
                , star_event_count
                , fork_event_count
                , pr_event_count
                , issues_event_count
                , push_event_count
                , created_date
            )
        SELECT
            repo_id
            , country_code
            , star_event_count
            , fork_event_count
            , pr_event_count
            , issues_event_count
            , push_event_count
            , created_date
        FROM {{ ti.xcom_pull(task_ids='staging_gold_repo_metrics') }}
        ON CONFLICT (repo_id, created_date, country_code)
        DO UPDATE SET
            repo_id = excluded.repo_id
            , country_code = excluded.country_code
            , star_event_count = excluded.star_event_count
            , fork_event_count = excluded.fork_event_count
            , pr_event_count = excluded.pr_event_count
            , issues_event_count = excluded.issues_event_count
            , push_event_count = excluded.push_event_count
            , created_date = excluded.created_date
        """,
        show_return_value_in_logs=True,
    )

    clear_staging_repo_metrics = SQLExecuteQueryOperator(
        task_id="clear_staging_repo_metrics",
        conn_id="postgres_default",
        sql="""DROP TABLE IF EXISTS {{ ti.xcom_pull(task_ids='staging_gold_repo_metrics') }}
        """,  # noqa: E501
    )

    (
        update_gold_repo_metrics
        >> staging_gold_repo_metrics
        >> merge_staging_repo_metrics_to_prod
        >> clear_staging_repo_metrics
    )
