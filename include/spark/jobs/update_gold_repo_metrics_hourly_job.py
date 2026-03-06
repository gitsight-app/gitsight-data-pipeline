from datetime import timedelta

import pendulum
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.types import IntegerType

from include.spark.common.decorators import spark_session_manager
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args
from include.spark.utils.col_utils import w_cols

source_fork_events_table_name = "nessie.gitsight.silver.fork_events"
source_watch_events_table_name = "nessie.gitsight.silver.watch_events"
dim_silver_repo_master_table_name = "nessie.gitsight.silver.repo_master"
target_gold_repo_metrics_table_name = "nessie.gitsight.gold.repo_metrics_hourly"


@spark_session_manager
def update_gold_repo_metrics_hourly_job(
    *, spark: SparkSession, data_interval_start, data_interval_end, logger, **kwargs
):
    start_ts = pendulum.parse(data_interval_start)
    end_ts = pendulum.parse(data_interval_end)

    date_between = (F.col("ingested_at") >= start_ts) & (F.col("ingested_at") < end_ts)

    fork_events = spark.read.table(source_fork_events_table_name).filter(date_between)
    watch_events = spark.read.table(source_watch_events_table_name).filter(date_between)

    logger.info(
        f"Fork, Watch Events Count: ({fork_events.count()}, {watch_events.count()})"
    )

    watch_events = watch_events.select(
        F.col("repo_id"),
        F.lit(1).alias("star_count"),
        F.lit(0).alias("fork_count"),
    )

    fork_events = fork_events.select(
        F.col("repo_id"),
        F.lit(0).alias("star_count"),
        F.lit(1).alias("fork_count"),
    )

    union_events = watch_events.union(fork_events)

    calc_count_df = union_events.groupBy(F.col("repo_id")).agg(
        F.sum("star_count").alias("star_count"),
        F.sum("fork_count").alias("fork_count"),
    )

    calc_count_df = calc_count_df.withColumn("ingested_at", F.lit(start_ts))

    repo_metrics_df = calc_count_df.select(
        "*",
        F.rank().over(Window.orderBy(F.col("star_count").desc())).alias("star_rank"),
        F.rank().over(Window.orderBy(F.col("fork_count").desc())).alias("fork_rank"),
    )

    repo_master_df = spark.read.table(dim_silver_repo_master_table_name).select(
        F.col("repo_id"),
        F.col("repo_name"),
    )

    final_df = (
        repo_metrics_df.alias("metrics")
        .join(
            repo_master_df.alias("repo"),
            F.col("metrics.repo_id") == F.col("repo.repo_id"),
            "left",
        )
        .select(F.col("repo.repo_name"), F.col("metrics.*"))
    )

    if not spark.catalog.tableExists(target_gold_repo_metrics_table_name):
        logger.info(f"Creating table {target_gold_repo_metrics_table_name}")
        _create_or_replace_gold_repo_metrics_table(final_df)
    else:
        logger.info(
            "Updating gold repo metrics table with new calculated metrics and trend"
        )
        update_gold_repo_metrics_table(
            spark=spark,
            df=final_df,
            curr_start_ts=start_ts,
            curr_end_ts=end_ts,
        )


def _create_or_replace_gold_repo_metrics_table(df: DataFrame):
    result_df = df.select(
        "*",
        F.lit(None).alias("prev_star_count").cast(IntegerType()),
        F.lit(None).alias("prev_fork_count").cast(IntegerType()),
        F.lit(None).alias("prev_star_rank").cast(IntegerType()),
        F.lit(None).alias("prev_fork_rank").cast(IntegerType()),
        F.lit(0).alias("star_trend").cast(IntegerType()),
        F.lit(0).alias("fork_trend").cast(IntegerType()),
        F.lit(True).alias("is_new"),
    ).orderBy(F.col("star_count").desc(), F.col("fork_count").desc())

    result_df = w_cols(
        result_df,
        ("ingested_date", F.to_date("ingested_at")),
        ("ingested_hour", F.hour("ingested_at")),
    )

    (
        result_df.writeTo("nessie.gitsight.gold.repo_metrics_hourly")
        .tableProperty("format-version", "2")
        .partitionedBy(F.col("ingested_date"), F.col("ingested_hour"))
        .createOrReplace()
    )


def update_gold_repo_metrics_table(
    spark: SparkSession,
    df: DataFrame,
    curr_start_ts: pendulum.datetime,
    curr_end_ts: pendulum.datetime,
):
    prev_start_ts = curr_start_ts - timedelta(hours=1)
    prev_end_ts = curr_end_ts - timedelta(hours=1)

    repo_metrics_hourly_df = spark.read.table(
        target_gold_repo_metrics_table_name
    ).filter(
        (F.col("ingested_at") >= prev_start_ts) & (F.col("ingested_at") < prev_end_ts)
    )

    calc_star_trend = F.coalesce(
        F.col("prev_star_rank"), F.col("curr_metrics.star_rank")
    ) - F.col("curr_metrics.star_rank")

    calc_fork_trend = F.coalesce(
        F.col("prev_fork_rank"), F.col("curr_metrics.fork_rank")
    ) - F.col("curr_metrics.fork_rank")

    result_df = (
        df.alias("curr_metrics")
        .join(
            repo_metrics_hourly_df.alias("prev_metrics"),
            on="repo_id",
            how="left",
        )
        .select(
            F.col("curr_metrics.repo_name"),
            F.col("repo_id"),
            F.col("curr_metrics.star_count"),
            F.col("curr_metrics.fork_count"),
            F.col("curr_metrics.star_rank"),
            F.col("curr_metrics.fork_rank"),
            F.col("prev_metrics.star_count").alias("prev_star_count"),
            F.col("prev_metrics.fork_count").alias("prev_fork_count"),
            F.col("prev_metrics.star_rank").alias("prev_star_rank"),
            F.col("prev_metrics.fork_rank").alias("prev_fork_rank"),
            calc_star_trend.alias("star_trend"),
            calc_fork_trend.alias("fork_trend"),
            F.col("curr_metrics.ingested_at").alias("ingested_at"),
        )
        .orderBy(
            F.col("curr_metrics.star_count").desc(),
            F.col("curr_metrics.fork_count").desc(),
        )
    )

    result_df = w_cols(
        result_df,
        ("ingested_date", F.to_date("ingested_at")),
        ("ingested_hour", F.hour("ingested_at")),
    )

    (
        result_df.writeTo("nessie.gitsight.gold.repo_metrics_hourly")
        .partitionedBy(F.col("ingested_date"), F.col("ingested_hour"))
        .overwritePartitions()
    )


if __name__ == "__main__":
    spark_session = SparkSessionFactory.create_session("UpdateGoldRepoMetricsHourlyJob")
    args = parse_required_args(["data_interval_start", "data_interval_end"])

    update_gold_repo_metrics_hourly_job(
        spark=spark_session,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
    )
