from enum import EnumType

from operators.spark.base.lake import CommonLakeSparkOperator


class EventType(EnumType):
    WATCH = "WatchEvent"
    FORK = "ForkEvent"
    PULL_REQUEST = "PullRequestEvent"
    PUSH = "PushEvent"
    ISSUES = "IssuesEvent"


class TransformSilverEventOperator(CommonLakeSparkOperator):
    template_fields = (*CommonLakeSparkOperator.template_fields,)

    def __init__(
        self,
        *,
        aws_conn_id="aws_default",
        catalog_conn_id="catalog_default",
        data_interval_start="{{ data_interval_start }}",
        data_interval_end="{{ data_interval_end }}",
        event_type: EventType,
        target_table: str,
        **kwargs,
    ):
        application_args = [
            "--data_interval_start",
            data_interval_start,
            "--data_interval_end",
            data_interval_end,
            "--event_type",
            event_type,
            "--target_table",
            target_table,
        ]

        super().__init__(
            application="/opt/airflow/include/spark/jobs/github_events_transform/transform_silver_events_from_bronze_job.py",
            application_args=application_args,
            aws_conn_id=aws_conn_id,
            catalog_conn_id=catalog_conn_id,
            **kwargs,
        )
