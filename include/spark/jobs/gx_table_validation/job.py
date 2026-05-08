import argparse

import pendulum
from great_expectations.core.batch import RuntimeBatchRequest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from include.spark.common.gx.checkpoint import GXCheckpoint
from include.spark.common.gx.context import get_gx_context
from include.spark.common.gx.expectation import GXExpectation
from include.spark.common.session_factory import SparkSessionFactory


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_table_name", required=True)
    parser.add_argument("--data_interval_start", required=False)
    parser.add_argument("--data_interval_end", required=False)
    parser.add_argument("--date_column", required=False)
    parser.add_argument("--suite_name", required=False)
    parser.add_argument("--checkpoint_name", required=False)
    parser.add_argument("--data_asset_name", required=False)
    return parser.parse_args()


def _filter_by_interval(
    df,
    *,
    date_column: str,
    data_interval_start: str | None,
    data_interval_end: str | None,
):
    if not date_column or not data_interval_start or not data_interval_end:
        return df

    start_ts = pendulum.parse(data_interval_start).start_of("hour")
    end_ts = pendulum.parse(data_interval_end).end_of("hour")

    column_type = dict(df.dtypes).get(date_column)
    if column_type == "date":
        start_value = start_ts.date()
        end_value = end_ts.date()
    else:
        start_value = start_ts
        end_value = end_ts

    return df.filter(F.col(date_column).between(start_value, end_value))


def _build_expectations(df, *, date_column: str | None):
    candidate_keys = ["id", "repo_id", "actor_id", "user_id", "event_id"]
    available_cols = set(df.columns)
    not_null_cols = [col for col in candidate_keys if col in available_cols]
    if date_column and date_column in available_cols:
        not_null_cols.append(date_column)

    ex_builder = GXExpectation.builder.table_row_count_between(min_value=0)
    if not_null_cols:
        ex_builder = ex_builder.column_not_null(not_null_cols)

    return ex_builder.build()


def run_validation(
    spark: SparkSession,
    *,
    source_table_name: str,
    data_interval_start: str | None,
    data_interval_end: str | None,
    date_column: str | None,
    suite_name: str | None,
    checkpoint_name: str | None,
    data_asset_name: str | None,
):
    df = spark.read.table(source_table_name)
    df = _filter_by_interval(
        df,
        date_column=date_column,
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
    )

    pod_name = spark.conf.get("spark.kubernetes.driver.pod.name", "local-run")

    batch_request = RuntimeBatchRequest(
        datasource_name="gitsight_datalake",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=data_asset_name or source_table_name,
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": pod_name},
    )

    context = get_gx_context()
    expectations = _build_expectations(df, date_column=date_column)
    suite_name = suite_name or f"{source_table_name}_suite"
    checkpoint_name = checkpoint_name or f"{source_table_name}_checkpoint"

    context.add_or_update_expectation_suite(
        expectation_suite_name=suite_name, expectations=expectations
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
        raise SystemExit(1)


if __name__ == "__main__":
    args = parse_args()
    spark_session = SparkSessionFactory.create_session("GxTableValidation")

    run_validation(
        spark_session,
        source_table_name=args.source_table_name,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
        date_column=args.date_column,
        suite_name=args.suite_name,
        checkpoint_name=args.checkpoint_name,
        data_asset_name=args.data_asset_name,
    )
