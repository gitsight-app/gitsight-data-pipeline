import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

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
FROM repo_metrics_hourly_staging
ON CONFLICT (repo_id, ingested_at)
DO UPDATE SET
    repo_name       = excluded.repo_name
    , star_count      = excluded.star_count
    , fork_count      = excluded.fork_count
    , star_rank       = excluded.star_rank
    , fork_rank       = excluded.fork_rank
    , prev_star_count = excluded.prev_star_count
    , prev_fork_count = excluded.prev_fork_count
    , prev_star_rank  = excluded.prev_star_rank
    , prev_fork_rank  = excluded.prev_fork_rank
    , star_trend      = excluded.star_trend
    , fork_trend      = excluded.fork_trend
    , is_new          = excluded.is_new
"""

with DAG(
    dag_id="update_repo_metrics_hourly",
    schedule=CronDataIntervalTimetable("20 * * * *", timezone=pendulum.UTC),
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
    template_searchpath=["/opt/airflow/include"],
) as dag:
    spark_application_base_path = "spark/jobs/update_repo_metrics_hourly"
    wait_for_silver_events = ExternalTaskSensor(
        task_id="wait_for_silver_events",
        external_dag_id="github_events_transform",
        external_task_id="end_events_transform",
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60,
    )

    update_gold_repo_metrics = SparkKubernetesOperator(
        task_id="update_gold_repo_metrics",
        application_file=f"{spark_application_base_path}/update_gold_repo_metrics/application.yaml",
        namespace="spark-applications",
    )

    load_oltp_gold_repo_metrics_hourly_to_staging = SparkKubernetesOperator(
        task_id="load_oltp_gold_repo_metrics_hourly_to_staging",
        application_file=f"{spark_application_base_path}/load_oltp_gold_repo_metrics_hourly_to_staging/application.yaml",
        params={
            "source_table_name": "nessie.gitsight.gold.repo_metrics_hourly",
            "target_table_name": "repo_metrics_hourly_staging",
        },
        namespace="spark-applications",
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
        sql="DROP TABLE IF EXISTS repo_metrics_hourly_staging"
    )

    (
        wait_for_silver_events
        >> update_gold_repo_metrics
        >> load_oltp_gold_repo_metrics_hourly_to_staging
        >> merge_staging_repo_metrics_to_prod
        >> clear_staging_repo_metrics_to_prod
    )
