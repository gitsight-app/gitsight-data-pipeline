from airflow.sdk import DAG
from operators.common.code_deploy import CodeDeployOperator
from operators.spark.base.lake import CommonLakeSparkOperator
from pendulum import datetime

with DAG(
    dag_id="repo_actor_master_transform",
    doc_md="""
    Loads the repository actor master data. to Silver layer.
    This DAG is scheduled to run daily and will process the data for the previous day.
    """,
    start_date=datetime(2026, 1, 1),
    schedule="15 * * * *",
    catchup=False,
) as dag:
    deploy_spark_code = CodeDeployOperator(
        task_id="deploy_spark_code",
        folder_path="/opt/airflow/include",
        s3_bucket="gitsight",
        s3_key="artifacts/builds/dev/include.zip",
        aws_conn_id="aws_default",
    )

    load_repo_master_to_silver = CommonLakeSparkOperator(
        task_id="load_repo_master_to_silver",
        py_files="{{ ti.xcom_pull(task_ids='deploy_spark_code') }}",
        application="/opt/airflow/include/spark/jobs/load_repo_master_to_silver_job.py",
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

    load_actor_master_to_silver = CommonLakeSparkOperator(
        task_id="load_actor_master_to_silver",
        py_files="{{ ti.xcom_pull(task_ids='deploy_spark_code') }}",
        application="/opt/airflow/include/spark/jobs/load_actor_master_to_silver_job.py",
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

    deploy_spark_code >> [load_repo_master_to_silver, load_actor_master_to_silver]
