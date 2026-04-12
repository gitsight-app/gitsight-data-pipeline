import pendulum
from great_expectations.core import ExpectationConfiguration
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import AbstractDataContext
from pyspark.sql import SparkSession

from include.spark.common.gx import get_gx_context
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args
from include.spark.utils.condition_utils import get_ingested_at_between_condition

gharchive_events_table_name = "nessie.gitsight.bronze.gharchive_events"

checkpoint_name = "nessie.gitsight.bronze.gharchive_events_checkpoint"
suite_name = "gharchive_events_suite"
data_asset_name = "bronze_gharchive_events"


def gx_extract_gharchive_events_to_bronze(
    spark: SparkSession, *, data_interval_start, data_interval_end
):
    start_ts = pendulum.parse(data_interval_start).start_of("hour")
    end_ts = pendulum.parse(data_interval_end).end_of("hour")

    target_df = spark.read.table(gharchive_events_table_name).where(
        get_ingested_at_between_condition(start_ts, end_ts)
    )
    pod_name = spark_session.conf.get("spark.kubernetes.driver.pod.name", "local-run")
    batch_request = RuntimeBatchRequest(
        datasource_name="gitsight_datalake",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=data_asset_name,
        runtime_parameters={"batch_data": target_df},
        batch_identifiers={"default_identifier_name": pod_name},
    )

    context = get_gx_context()

    validate_dataframe(context=context, run_name=pod_name, batch_request=batch_request)


def validate_dataframe(
    context: AbstractDataContext, run_name: str, batch_request: RuntimeBatchRequest
):
    """
    Create New Checkpoint with Suite
    :param run_name: Identify Run in GX
    :param batch_request: Batch Request for Validation
    :param context: GxContext
    :return: checkpoint
    """
    suite = context.add_or_update_expectation_suite(suite_name)
    suite.expectations = []

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "ingested_at",
            },
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "id"},
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 0, "max_value": None},
            meta={
                "description": "Row Count should be greater than or equal to 0",
                "source_table": gharchive_events_table_name,
            },
        )
    )

    context.update_expectation_suite(suite)

    checkpoint = context.add_or_update_checkpoint(
        name=checkpoint_name,
        validations=[
            {
                "expectation_suite_name": suite_name,
            }
        ],
        action_list=[
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
        ],
    )

    checkpoint.run(
        run_name=run_name,
        batch_request=batch_request,
    )


if __name__ == "__main__":
    spark_session = SparkSessionFactory.create_session("GxExtractGhArchiveEventsToBronze")
    args = parse_required_args(["data_interval_start", "data_interval_end"])

    gx_extract_gharchive_events_to_bronze(
        spark=spark_session,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
    )
