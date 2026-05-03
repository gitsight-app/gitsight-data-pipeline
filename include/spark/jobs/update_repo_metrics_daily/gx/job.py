from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import AbstractDataContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from include.spark.common.gx.checkpoint import GXCheckpoint
from include.spark.common.gx.context import get_gx_context
from include.spark.common.gx.expectation import GXExpectation
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args

data_asset_name = "gold_repo_metrics_daily"
suite_name = "repo_metrics_daily_suite"
checkpoint_name = "nessie.gitsight.gold.repo_metrics_daily_checkpoint"


def gx_update_repo_metrics_daily(
    spark: SparkSession, *, target_date, source_table_name
):

    target_df = spark.read.table(source_table_name).filter(
        F.col("created_date") == target_date
    )
    pod_name = spark.conf.get("spark.kubernetes.driver.pod.name", "local-run")

    batch_request = RuntimeBatchRequest(
        datasource_name="gitsight_datalake",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=data_asset_name,
        runtime_parameters={"batch_data": target_df},
        batch_identifiers={"default_identifier_name": pod_name},
    )

    context = get_gx_context()

    is_success = run_validate_by_gx(
        context=context, run_name=pod_name, batch_request=batch_request
    )

    if not is_success:
        exit(1)


def run_validate_by_gx(
    context: AbstractDataContext, run_name: str, batch_request: RuntimeBatchRequest
) -> bool:

    exs = (
        GXExpectation.builder.column_not_null("repo_id")
        .column_unique("repo_id")
        .column_greater_than_zero(["star_count"])
        .table_row_count_between(min_value=1)
        .build()
    )

    context.add_or_update_expectation_suite(
        expectation_suite_name=suite_name, expectations=exs
    )

    checkpoint = (
        GXCheckpoint.builder.name(checkpoint_name)
        .suites(suite_name)
        .store_validation_result()
        .update_data_docs()
        .build_and_update(context)
    )

    result = checkpoint.run(
        run_name=run_name,
        batch_request=batch_request,
    )

    return result.success


def create_expectation_suite(context: AbstractDataContext):

    exs = (
        GXExpectation.builder.column_not_null("repo_id")
        .column_values_to_be_unique("repo_id")
        .build()
    )

    context.add_or_update_expectation_suite(
        expectation_suite_name=suite_name, expectations=exs
    )


if __name__ == "__main__":
    spark_session = SparkSessionFactory.create_session("GxUpdateRepoMetricsDaily")
    args = parse_required_args(["target_date", "source_table_name"])

    gx_update_repo_metrics_daily(
        spark=spark_session,
        target_date=args.target_date,
        source_table_name=args.source_table_name,
    )
