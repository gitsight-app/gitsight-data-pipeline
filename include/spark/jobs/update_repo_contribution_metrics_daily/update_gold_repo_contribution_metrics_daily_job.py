import pendulum
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

from include.spark.common.decorators import spark_session_manager
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args
from include.spark.utils.condition_utils import get_ingested_at_between_condition

source_silver_actor_detail_scd_table_name = "nessie.gitsight.silver.actor_detail_scd"
source_silver_unified_events_view_name = "nessie.gitsight.silver.unified_events"

target_gold_repo_contribution_metrics_daily_table_name = (
    "nessie.gitsight.gold.repo_contribution_metrics_daily"  # noqa: E501
)


@spark_session_manager
def update_gold_repo_contribution_metrics_daily_job(
    *, spark: SparkSession, data_interval_start, data_interval_end, logger, **kwargs
):
    start_ts = pendulum.parse(data_interval_start).start_of("day")
    end_ts = pendulum.parse(data_interval_end).start_of("day")

    date_between = get_ingested_at_between_condition(start_ts, end_ts)

    events_df = spark.read.table(source_silver_unified_events_view_name).where(
        date_between
    )

    actor_detail_scd_df = spark.read.table(
        source_silver_actor_detail_scd_table_name
    ).where((F.col("ingested_at") < F.lit(end_ts)))

    events_with_actor_detail_df = join_events_actor_df(
        events_df, "events", actor_detail_scd_df, "actors"
    )
    result_df = events_with_actor_detail_df.groupBy(
        F.col("repo_id"), F.col("country_code")
    ).agg(*get_events_type_count_cols())

    result_df = result_df.withColumn("created_date", F.lit(start_ts.date()))

    if not spark.catalog.tableExists(
        target_gold_repo_contribution_metrics_daily_table_name
    ):
        (
            result_df.writeTo(target_gold_repo_contribution_metrics_daily_table_name)
            .tableProperty("format-version", "2")
            .partitionedBy(F.col("created_date"))
            .create()
        )
    else:
        (
            result_df.writeTo(
                target_gold_repo_contribution_metrics_daily_table_name
            ).overwritePartitions()
        )


def join_events_actor_df(events_df, events_alias, actor_df, actor_alias):

    created_at_is_between_valid = (
        F.col(f"{events_alias}.created_at") >= F.col(f"{actor_alias}.valid_from")
    ) & (F.col(f"{events_alias}.created_at") < F.col(f"{actor_alias}.valid_to"))
    event_actor_join_condition = (
        F.col(f"{events_alias}.actor_id") == F.col(f"{actor_alias}.user_id")
    ) & created_at_is_between_valid

    return events_df.alias(events_alias).join(
        actor_df.alias(actor_alias).hint("shuffle_hash"),
        on=event_actor_join_condition,
        how="left",
    )


def get_events_type_count_cols() -> list[Column]:
    return [
        F.sum(F.when(F.col("event_type") == "WatchEvent", 1).otherwise(0)).alias(
            "star_event_count"
        ),
        F.sum(F.when(F.col("event_type") == "ForkEvent", 1).otherwise(0)).alias(
            "fork_event_count"
        ),
        F.sum(F.when(F.col("event_type") == "PullRequestEvent", 1).otherwise(0)).alias(
            "pr_event_count"
        ),
        F.sum(F.when(F.col("event_type") == "IssuesEvent", 1).otherwise(0)).alias(
            "issues_event_count"
        ),
        F.sum(F.when(F.col("event_type") == "PushEvent", 1).otherwise(0)).alias(
            "push_event_count"
        ),
    ]


if __name__ == "__main__":
    args = parse_required_args(["data_interval_start", "data_interval_end"])
    spark_session = SparkSessionFactory.create_session(
        "UpdateGoldRepoContributionMetricsDailyJob"
    )

    update_gold_repo_contribution_metrics_daily_job(
        spark=spark_session,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
    )
