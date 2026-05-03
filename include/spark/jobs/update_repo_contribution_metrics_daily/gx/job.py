from great_expectations.core.batch import RuntimeBatchRequest
from pyspark.sql import functions as F

from include.spark.common.gx.checkpoint import GXCheckpoint
from include.spark.common.gx.context import get_gx_context
from include.spark.common.gx.expectation import GXExpectation
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args

source_table_name = "nessie.gitsight.gold.repo_contribution_metrics_daily"
data_asset_name = "gold_repo_contribution_metrics_daily"
suite_name = "repo_contribution_metrics_daily_suite"
checkpoint_name = "nessie.gitsight.gold.repo_contribution_metrics_checkpoint"


def gx_update_repo_contribution_metrics_daily_job(spark, target_date):
    pod_name = spark.conf.get("spark.kubernetes.driver.pod.name", "local-run")

    source_df = spark.read.table(source_table_name).where(
        F.col("created_date") == F.lit(target_date)
    )

    source_df = source_df.withColumn(
        "total_event_count",
        F.col("star_event_count")
        + F.col("fork_event_count")
        + F.col("pr_event_count")
        + F.col("issues_event_count")
        + F.col("push_event_count"),
    )

    context = get_gx_context()

    batch_request = RuntimeBatchRequest(
        datasource_name="gitsight_datalake",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=data_asset_name,
        runtime_parameters={"batch_data": source_df},
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    exs = (
        GXExpectation.builder.column_greater_than_zero(
            "total_event_count"
        ).column_not_null(["repo_id", "country_code", "created_date"])
    ).build()

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
        run_name=pod_name,
        batch_request=batch_request,
    )

    if not result.success:
        exit(1)


if __name__ == "__main__":
    args = parse_required_args(["target_date"])
    spark_session = SparkSessionFactory.create_session(
        "GxUpdateRepoContributionMetricsDail"
    )

    gx_update_repo_contribution_metrics_daily_job(
        spark=spark_session,
        target_date=args.target_date,
    )
