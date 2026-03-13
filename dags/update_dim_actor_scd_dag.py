from airflow.sdk import DAG
from operators.common.code_deploy import CodeDeployOperator
from operators.spark.base.lake import CommonLakeSparkOperator
from operators.spark.github_api import FetchGithubAPISparkOperator
from pendulum import datetime

with DAG(
    dag_id="update_dim_actor_scd",
    schedule="50 * * * *",
    start_date=datetime(2026, 1, 1),
    max_active_tasks=1,
    catchup=False,
) as dag:
    spark_job_base_path = "/opt/airflow/include/spark/jobs/update_dim_actor_scd"
    py_file_xcom = "{{ ti.xcom_pull(task_ids='deploy_spark_code') }}"

    deploy_spark_code = CodeDeployOperator(
        task_id="deploy_spark_code",
        folder_path="/opt/airflow/include",
        s3_bucket="gitsight",
        s3_key="artifacts/builds/dev/include.zip",
        aws_conn_id="aws_default",
    )

    fetch_actor_detail_to_raw = FetchGithubAPISparkOperator(
        task_id="fetch_actor_detail_to_raw",
        application=f"{spark_job_base_path}/fetch_actor_detail_to_raw_job.py",
        py_files=py_file_xcom,
        executor_memory="2g",
        executor_cores=2,
        github_api_conn_id="github_api",
        application_args=[
            "--target_datetime",
            "{{ data_interval_start.start_of('hour') }}",
        ],
    )

    merge_dim_actor_scd = CommonLakeSparkOperator(
        task_id="merge_dim_actor_scd",
        application=f"{spark_job_base_path}/merge_dim_actor_scd_job.py",
        py_files=py_file_xcom,
        application_args=[
            "--target_datetime",
            "{{ data_interval_start.start_of('hour') }}",
        ],
        aws_conn_id="aws_default",
        catalog_conn_id="catalog_default",
    )

    deploy_spark_code >> fetch_actor_detail_to_raw >> merge_dim_actor_scd
