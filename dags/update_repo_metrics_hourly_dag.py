import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG
from operators.common.code_deploy import CodeDeployOperator
from operators.spark.base.lake import CommonLakeSparkOperator
from operators.spark.load_to_oltp import LoadToOltpOperator

UPSERT_QUERY = """
INSERT INTO repo_metrics_hourly (
    repo_name
    , repo_id
    , star_count
    , fork_count
    , ingested_at
    ,star_rank
    , fork_rank
    , prev_star_count
    , prev_fork_count
    , prev_star_rank
    , prev_fork_rank
    , star_trend
    , fork_trend
    , is_new
    , ingested_date
    , ingested_hour
)
SELECT
    repo_name
    , repo_id
    , star_count
    , fork_count
    , ingested_at
    ,star_rank
    , fork_rank
    , prev_star_count
    , prev_fork_count
    , prev_star_rank
    , prev_fork_rank
    , star_trend
    , fork_trend
    , is_new
    , ingested_date
    , ingested_hour
FROM {{ ti.xcom_pull(task_ids='staging_gold_repo_metrics_table') }}
ON CONFLICT (repo_id, ingested_at)
DO UPDATE set
    repo_name       = EXCLUDED.repo_name
    , star_count      = EXCLUDED.star_count
    , fork_count      = EXCLUDED.fork_count
    , star_rank       = EXCLUDED.star_rank
    , fork_rank       = EXCLUDED.fork_rank
    , prev_star_count = EXCLUDED.prev_star_count
    , prev_fork_count = EXCLUDED.prev_fork_count
    , prev_star_rank  = EXCLUDED.prev_star_rank
    , prev_fork_rank  = EXCLUDED.prev_fork_rank
    , star_trend      = EXCLUDED.star_trend
    , fork_trend      = EXCLUDED.fork_trend
    , is_new          = EXCLUDED.is_new
    , ingested_date   = EXCLUDED.ingested_date
    , ingested_hour   = EXCLUDED.ingested_hour;
"""

with DAG(
    dag_id="update_repo_metrics_hourly_dag",
    schedule="20 * * * *",
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
) as dag:
    wait_for_silver_events = ExternalTaskSensor(
        task_id="wait_for_silver_events",
        external_dag_id="github_events_transform",
        external_task_id="end_events_transform",
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60,
    )

    deploy_spark_code = CodeDeployOperator(
        task_id="deploy_spark_code",
        folder_path="/opt/airflow/include",
        s3_bucket="gitsight",
        s3_key="artifacts/builds/dev/include.zip",
        aws_conn_id="aws_default",
    )

    update_gold_repo_metrics_table = CommonLakeSparkOperator(
        task_id="update_gold_repo_metrics_table",
        py_files="{{ ti.xcom_pull(task_ids='deploy_spark_code') }}",
        application="/opt/airflow/include/spark/jobs/update_gold_repo_metrics_hourly_job.py",
        application_args=[
            "--data_interval_start",
            "{{ data_interval_start }}",
            "--data_interval_end",
            "{{ data_interval_end }}",
        ],
        executor_memory="2g",
        aws_conn_id="aws_default",
        catalog_conn_id="catalog_default",
        verbose=True,
    )

    staging_gold_repo_metrics_table = LoadToOltpOperator(
        task_id="staging_gold_repo_metrics_table",
        data_interval_start="{{ data_interval_start }}",
        data_interval_end="{{ data_interval_end }}",
        application="/opt/airflow/include/spark/jobs/load_oltp_gold_repo_metrics_hourly_to_staging_job.py",
        aws_conn_id="aws_default",
        catalog_conn_id="catalog_default",
        jdbc_conn_id="postgres_default",
        staging_table_name="repo_metrics_hourly_staging_{{ ts_nodash }}",
    )

    merge_staging_repo_metrics_to_prod = SQLExecuteQueryOperator(
        task_id="merge_staging_repo_metrics_to_prod",
        conn_id="postgres_default",
        sql=UPSERT_QUERY,
        show_return_value_in_logs=True,
    )

    clear_staging_repo_metrics_to_prod = SQLExecuteQueryOperator(
        task_id="clear_staging_repo_metrics_to_prod",
        conn_id="postgres_default",
        sql="DROP TABLE if EXISTS {{ ti.xcom_pull(task_ids='staging_gold_repo_metrics_table') }}",  # noqa: E501
    )

    (
        wait_for_silver_events
        >> deploy_spark_code
        >> update_gold_repo_metrics_table
        >> staging_gold_repo_metrics_table
        >> merge_staging_repo_metrics_to_prod
        >> clear_staging_repo_metrics_to_prod
    )
