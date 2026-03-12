import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG
from operators.common.code_deploy import CodeDeployOperator
from operators.spark.base.lake import CommonLakeSparkOperator
from operators.spark.oltp_staging import LakeToOLTPStagingOperator

with DAG(
    dag_id="update_repo_metrics_daily",
    doc_md="""
    - Update repo metrics daily to calculate daily star and fork count, rank and trend.
    - This DAG is scheduled to run daily and will process the data for the previous day.
    """,
    start_date=pendulum.datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    gold_repo_metrics_table_name = "nessie.gitsight.gold.repo_metrics_daily"
    spark_job_bast_path = "/opt/airflow/include/spark/jobs/update_repo_metrics_daily"

    deploy_spark_code = CodeDeployOperator(
        task_id="deploy_spark_code",
        folder_path="/opt/airflow/include",
        s3_bucket="gitsight",
        s3_key="artifacts/builds/dev/include.zip",
        aws_conn_id="aws_default",
    )

    update_gold_repo_metrics_daily = CommonLakeSparkOperator(
        task_id="update_gold_repo_metrics_daily",
        py_files="{{ ti.xcom_pull(task_ids='deploy_spark_code') }}",
        application=f"{spark_job_bast_path}/update_gold_repo_metrics_daily_job.py",
        application_args=[
            "--target_date",
            "{{ ds }}",
            "--target_table_name",
            gold_repo_metrics_table_name,
        ],
        executor_memory="2g",
        aws_conn_id="aws_default",
        catalog_conn_id="catalog_default",
        verbose=True,
    )

    load_oltp_gold_repo_metrics_hourly_to_staging = LakeToOLTPStagingOperator(
        task_id="load_oltp_gold_repo_metrics_hourly_to_staging",
        application=f"{spark_job_bast_path}/load_oltp_gold_repo_metrics_daily_to_staging_job.py",
        staging_table_name="repo_metrics_daily_staging",
        application_args=[
            "--target_date",
            "{{ ds }}",
        ],
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
        deploy_spark_code
        >> update_gold_repo_metrics_daily
        >> load_oltp_gold_repo_metrics_hourly_to_staging
        >> merge_staging_repo_metrics_to_prod
        >> clear_staging_repo_metrics
    )
