import logging

import pendulum
from great_expectations.core.batch import RuntimeBatchRequest

from include.spark.common.gx.checkpoint import GXCheckpoint
from include.spark.common.gx.context import get_gx_context
from include.spark.common.gx.expectation import GXExpectation
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args
from include.spark.utils.condition_utils import get_ingested_at_between_condition

source_table_name = "nessie.gitsight.gold.repo_metrics_hourly"
data_asset_name = "gold_repo_metrics_hourly"
suite_name = "repo_metrics_hourly_suite"
checkpoint_name = "repo_metrics_hourly_checkpoint"


def gx_update_repo_metrics_hourly(
    spark,
    *,
    data_interval_start,
    data_interval_end,
):
    start_ts = pendulum.parse(data_interval_start).start_of("hour")
    end_ts = pendulum.parse(data_interval_end).start_of("hour")
    pod_name = spark.conf.get("spark.kubernetes.driver.pod.name", "local-run")

    df = spark.read.table(source_table_name).filter(
        get_ingested_at_between_condition(start_ts, end_ts)
    )

    batch_request = RuntimeBatchRequest(
        datasource_name="gitsight_datalake",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=data_asset_name,
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    context = get_gx_context()

    max_rank = df.count()

    exs = (
        GXExpectation.builder.column_not_null(["repo_id", "ingested_at"])
        .column_greater_than_zero(["fork_rank", "star_rank"])
        .column_value_between(
            columns=["star_rank", "fork_rank"], min_value=1, max_value=max_rank
        )
        .table_row_count_between(min_value=1)
    ).build()

    context.add_or_update_expectation_suite(
        expectation_suite_name=suite_name, expectations=exs
    )

    checkpoint = (
        GXCheckpoint.builder.name(checkpoint_name)
        .suites(suite_name)
        .build_and_update(context)
    )

    result = checkpoint.run(
        run_name=pod_name,
        batch_request=batch_request,
    )

    if not result.success:
        logging.warn("Failed to update metrics hourly")
        exit(1)


if __name__ == "__main__":
    spark_session = SparkSessionFactory.create_session("GxUpdateRepoMetricsHourly")
    args = parse_required_args(["data_interval_start", "data_interval_end"])

    gx_update_repo_metrics_hourly(
        spark=spark_session,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
    )
