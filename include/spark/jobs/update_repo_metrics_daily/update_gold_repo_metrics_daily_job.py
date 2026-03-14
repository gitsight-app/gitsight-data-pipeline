from typing import Union

import pendulum
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.session import SparkSession

from include.spark.common.decorators import spark_session_manager
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args

SOURCE_EVENT_TABLE_NAMES = [
    "nessie.gitsight.silver.watch_events",
    "nessie.gitsight.silver.fork_events",
    "nessie.gitsight.silver.pull_request_events",
    "nessie.gitsight.silver.issues_events",
    "nessie.gitsight.silver.push_events",
]


@spark_session_manager
def update_gold_repo_metrics_daily_job(
    *,
    spark: SparkSession,
    target_date,
    target_table_name,
    logger,
    **kwargs,
):
    logger.info(f"Start to update gold repo metrics for date: {target_date}")
    start_ts = pendulum.parse(target_date).start_of("day")
    end_ts = pendulum.parse(target_date).end_of("day")

    events_df = spark.read.table(SOURCE_EVENT_TABLE_NAMES[0])

    date_between = (F.col("ingested_at") >= start_ts) & (F.col("ingested_at") < end_ts)

    for table_name in SOURCE_EVENT_TABLE_NAMES[1:]:
        events_df = _union_source_events(spark, events_df, table_name, date_between)

    events_with_date_df = events_df.withColumn("created_date", F.to_date("created_at"))

    event_count_by_repo_id_per_day_df = events_with_date_df.groupBy(
        F.col("repo_id"), F.col("created_date")
    ).agg(
        F.sum(
            F.when(F.col("event_type") == "WatchEvent", 1).otherwise(0),
        ).alias("star_count"),
        F.sum(
            F.when(F.col("event_type") == "ForkEvent", 1).otherwise(0),
        ).alias("fork_count"),
        F.sum(
            F.when(F.col("event_type") == "PullRequestEvent", 1).otherwise(0),
        ).alias("pr_count"),
        F.sum(
            F.when(F.col("event_type") == "IssuesEvent", 1).otherwise(0),
        ).alias("issues_count"),
        F.sum(
            F.when(F.col("event_type") == "PushEvent", 1).otherwise(0),
        ).alias("push_count"),
    )

    result_df = event_count_by_repo_id_per_day_df.orderBy(
        F.col("star_count").desc(), F.col("repo_id")
    )

    if not spark.catalog.tableExists(target_table_name):
        (
            result_df.writeTo(target_table_name)
            .tableProperty("format-version", "2")
            .partitionedBy(F.days("created_date"))
            .create()
        )
    else:
        result_df.writeTo(target_table_name).overwritePartitions()


def _union_source_events(
    spark, df: DataFrame, df2_name: str, condition: Union[Column, str]
) -> DataFrame:
    """
    df.union(df2)
    :param spark:
    :param df:
    :param df2_name:
    :param target_date:
    :return:
    """
    df2 = spark.read.table(df2_name).where(condition)

    return df.unionAll(df2)


if __name__ == "__main__":
    spark_session = SparkSessionFactory.create_session("UpdateGoldRepoMetricsDailyJob")
    args = parse_required_args(["target_date", "target_table_name"])

    update_gold_repo_metrics_daily_job(
        spark=spark_session,
        target_date=args.target_date,
        target_table_name=args.target_table_name,
    )
