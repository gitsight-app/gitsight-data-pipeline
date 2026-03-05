from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from hook.gh_archive import GHArchiveHook
from operators.common.code_deploy import CodeDeployOperator
from operators.spark.ExtractMetaOperator import ExtractMetaOperator
from operators.spark.lake import CommonLakeSparkOperator
from pendulum import datetime


def _save_gharchive_to_s3(*, aws_conn_id: str, bucket_name: str, **context):
    hook = GHArchiveHook(aws_conn_id=aws_conn_id, bucket_name=bucket_name)
    date: datetime = context["data_interval_start"]

    return hook.save_archive(date)


with DAG(
    dag_id="gharchive_events_ingest",
    doc_md="""
    - Ingest Gharchive data from https://www.gharchive.org/
    - extract actor and repo meta from gharchive events in bronze layer
    """,
    start_date=datetime(2026, 1, 1),
    schedule="30 * * * *",
    catchup=False,
) as dag:
    deploy_spark_code = CodeDeployOperator(
        task_id="deploy_spark_code",
        folder_path="/opt/airflow/include",
        s3_bucket="gitsight",
        s3_key="artifacts/builds/dev/include.zip",
        aws_conn_id="aws_default",
    )

    save_gharchive_to_s3 = PythonOperator(
        task_id="save_gharchive_to_s3",
        python_callable=_save_gharchive_to_s3,
        op_kwargs={
            "aws_conn_id": "aws_default",
            "bucket_name": "gitsight",
        },
    )

    extract_gharchive_event_to_bronze = CommonLakeSparkOperator(
        task_id="extract_gharchive_event_to_bronze",
        py_files="{{ ti.xcom_pull(task_ids='deploy_spark_code') }}",
        application="/opt/airflow/include/spark/jobs/extract_gharchive_events_to_bronze_job.py",
        application_args=[
            "--source_path",
            "{{ ti.xcom_pull(task_ids='save_gharchive_to_s3') }}",
            "--data_interval_start",
            "{{ data_interval_start }}",
        ],
        executor_memory="1g",
        aws_conn_id="aws_default",
        catalog_conn_id="catalog_default",
        verbose=True,
    )

    extract_actor_meta_from_bronze_events = ExtractMetaOperator(
        task_id="extract_actor_meta_from_bronze_events",
        py_files="{{ ti.xcom_pull(task_ids='deploy_spark_code') }}",
        application="/opt/airflow/include/spark/jobs/extract_actor_meta_from_bronze_events_job.py",
        verbose=True,
    )

    extract_repo_meta_from_bronze_events = ExtractMetaOperator(
        task_id="extract_repo_meta_from_bronze_events",
        py_files="{{ ti.xcom_pull(task_ids='deploy_spark_code') }}",
        application="/opt/airflow/include/spark/jobs/extract_repo_meta_from_bronze_events_job.py",
        verbose=True,
    )

    (
        deploy_spark_code
        >> save_gharchive_to_s3
        >> extract_gharchive_event_to_bronze
        >> [extract_actor_meta_from_bronze_events, extract_repo_meta_from_bronze_events]
    )
