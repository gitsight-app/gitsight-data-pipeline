import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import datetime

with DAG(
    dag_id="repo_actor_master_transform",
    doc_md="""
    Loads the repository actor master data. to Silver layer.
    This DAG is scheduled to run daily and will process the data for the previous day.
    """,
    start_date=datetime(2026, 1, 1),
    schedule=CronDataIntervalTimetable("15 * * * *", timezone=pendulum.UTC),
    template_searchpath=["/opt/airflow/include"],
    catchup=False,
) as dag:
    load_repo_master_to_silver = SparkKubernetesOperator(
        task_id="load_repo_master_to_silver",
        application_file="spark/jobs/repo_actor_master_transform/load_actor_master_to_silver/application.yaml",
        namespace="spark-applications",
    )

    load_actor_master_to_silver = SparkKubernetesOperator(
        task_id="load_actor_master_to_silver",
        application_file="spark/jobs/repo_actor_master_transform/load_actor_master_to_silver/application.yaml",
        namespace="spark-applications",
    )
