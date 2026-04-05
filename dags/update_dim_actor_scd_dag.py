import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import datetime

with DAG(
    dag_id="update_dim_actor_scd",
    schedule=CronDataIntervalTimetable("50 * * * *", timezone=pendulum.UTC),
    start_date=datetime(2026, 1, 1),
    max_active_tasks=1,
    catchup=False,
    template_searchpath=["/opt/airflow/include"],
) as dag:
    fetch_actor_detail_to_raw = SparkKubernetesOperator(
        task_id="fetch_actor_detail_to_raw",
        application_file="spark/jobs/update_dim_actor_scd/fetch_actor_detail_to_raw/application.yaml",
        namespace="spark-applications",
    )

    merge_dim_actor_scd = SparkKubernetesOperator(
        task_id="merge_dim_actor_scd",
        application_file="spark/jobs/update_dim_actor_scd/merge_dim_actor_scd/application.yaml",
        namespace="spark-applications",
    )

    fetch_actor_detail_to_raw >> merge_dim_actor_scd
