import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG

with DAG(
    dag_id="update_repo_metrics_daily",
    doc_md="""
    - Update repo metrics daily to calculate daily star and fork count, rank and trend.
    - This DAG is scheduled to run daily and will process the data for the previous day.
    """,
    start_date=pendulum.datetime(2026, 1, 1),
    schedule="@daily",
    template_searchpath=["/opt/airflow/include"],
    catchup=False,
) as dag:
    gold_repo_metrics_table_name = "nessie.gitsight.gold.repo_metrics_daily"
    spark_job_bast_path = "/opt/airflow/include/spark/jobs/update_repo_metrics_daily"

    update_gold_repo_metrics_daily = SparkKubernetesOperator(
        task_id="update_gold_repo_metrics_daily",
        application_file="spark/jobs/update_gold_repo_metrics_daily/application.yaml",
        namespace="spark_applications",
        params={"target_table_name": "nessie.gitsight.gold.repo_metrics_daily"},
    )

    load_oltp_gold_repo_metrics_hourly_to_staging = SparkKubernetesOperator(
        task_id="load_oltp_gold_repo_metrics_hourly_to_staging",
        application_file="spark/jobs/load_to_oltp_staging_daily/application.yaml",
        namespace="spark_applications",
        params={
            "source_table_name": "nessie.gitsight.gold.repo_metrics_daily",
            "staging_table_name": "repo_metrics_daily_staging",
            "date_condition_col_name": "created_date",
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
        FROM {{ ti.xcom_pull(task_ids='load_oltp_gold_repo_metrics_hourly_to_staging') }}
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
        sql="""DROP TABLE IF EXISTS {{ ti.xcom_pull(task_ids='load_oltp_gold_repo_metrics_hourly_to_staging') }}
        """,  # noqa: E501
    )

    (
        update_gold_repo_metrics_daily
        >> load_oltp_gold_repo_metrics_hourly_to_staging
        >> merge_staging_repo_metrics_to_prod
        >> clear_staging_repo_metrics
    )
