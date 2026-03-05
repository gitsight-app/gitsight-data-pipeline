from airflow.sdk import DAG
from operators.common.code_deploy import CodeDeployOperator
from operators.spark.lake import CommonLakeSparkOperator
from pendulum import datetime

with DAG(
    dag_id="github_events_transform",
    doc_md="""
    Transform github events data in silver layer from bronze gharchive events table
    - Watch Events(Star Events)

    """,
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
) as dag:
    deploy_spark_code = CodeDeployOperator(
        task_id="deploy_spark_code",
        folder_path="/opt/airflow/include",
        s3_bucket="gitsight",
        s3_key="artifacts/builds/dev/include.zip",
        aws_conn_id="aws_default",
    )

    transform_silver_watch_events_from_bronze = CommonLakeSparkOperator(
        task_id="transform_silver_watch_events_from_bronze",
        py_files="{{ ti.xcom_pull(task_ids='deploy_spark_code') }}",
        application="/opt/airflow/include/spark/jobs/transform_silver_watch_events_from_bronze_job.py",
        application_args=[
            "--data_interval_start",
            "{{ data_interval_start }}",
            "--data_interval_end",
            "{{ data_interval_end }}",
        ],
        executor_memory="1g",
        aws_conn_id="aws_default",
        catalog_conn_id="catalog_default",
        verbose=True,
    )

    deploy_spark_code >> transform_silver_watch_events_from_bronze
