from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG
from operators.common.code_deploy import CodeDeployOperator
from operators.spark.base.lake import CommonLakeSparkOperator
from operators.spark.oltp_staging import LakeToOLTPStagingDailyOperator
from pendulum import datetime

with DAG(
    dag_id="update_repo_contribution_metrics_daily",
    start_date=datetime(2026, 1, 1),
    schedule="10 0 * * *",
    catchup=False,
) as dag:
    spark_job_base_path = (
        "/opt/airflow/include/spark/jobs/update_repo_contribution_metrics_daily"
    )
    py_file_xcom = "{{ ti.xcom_pull(task_ids='deploy_spark_code') }}"

    deploy_spark_code = CodeDeployOperator(
        task_id="deploy_spark_code",
        folder_path="/opt/airflow/include",
        s3_bucket="gitsight",
        s3_key="artifacts/builds/dev/include.zip",
        aws_conn_id="aws_default",
    )

    update_gold_repo_metrics = CommonLakeSparkOperator(
        task_id="update_gold_repo_metrics",
        application=f"{spark_job_base_path}/update_gold_repo_contribution_metrics_daily_job.py",
        py_files=py_file_xcom,
        application_args=[
            "--data_interval_start",
            "{{ data_interval_start }}",
            "--data_interval_end",
            "{{ data_interval_end }}",
        ],
        aws_conn_id="aws_default",
        catalog_conn_id="catalog_default",
    )

    staging_gold_repo_metrics = LakeToOLTPStagingDailyOperator(
        task_id="staging_gold_repo_metrics",
        source_table_name="nessie.gitsight.gold.repo_contribution_metrics_daily",
        staging_table_name="repo_contribution_metrics_daily_staging",
        date_condition_col_name="created_date",
        target_date="{{ ds }}",
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
        deploy_spark_code
        >> update_gold_repo_metrics
        >> staging_gold_repo_metrics
        >> merge_staging_repo_metrics_to_prod
        >> clear_staging_repo_metrics
    )
