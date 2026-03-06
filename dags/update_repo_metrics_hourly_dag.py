import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG
from operators.common.code_deploy import CodeDeployOperator
from operators.spark.lake import CommonLakeSparkOperator

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

    dump_repo_metrics_to_oltp = EmptyOperator(task_id="dump_repo_metrics_to_oltp")

    (
        wait_for_silver_events
        >> deploy_spark_code
        >> update_gold_repo_metrics_table
        >> dump_repo_metrics_to_oltp
    )
