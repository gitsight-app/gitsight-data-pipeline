from datetime import timedelta

import pendulum
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from include.spark.common.decorators import spark_session_manager
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args

bronze_gharchive_events_table_name = "nessie.gitsight.bronze.gharchive_events"
target_silver_watch_events_table_name = "nessie.gitsight.silver.watch_events"


@spark_session_manager
def transform_silver_watch_events_from_bronze_job(
    spark: SparkSession, data_interval_start, data_interval_end, logger, **kwargs
):
    """
    Extract Watch Events(Star Events)
    from bronze gharchive events table

    Node:
        - Partition overwrite By ingested_date and ingested_hour
        - Sliding Window Recompute (2 hours) 11:00-12:00 then window(09:00-12:00)
    :param spark: Spark session
    :param data_interval_start: 2026-03-03 11:00:00
    :param data_interval_end: 2026-03-03 12:00:00
    :param logger: injected from decorator (spark_session_manager)
    :param kwargs:
    :return:
    """  # noqa: E501
    start_ts = pendulum.parse(data_interval_start) - timedelta(hours=1)
    end_ts = pendulum.parse(data_interval_end)

    logger.info(f"Transform Between Date: {start_ts} - {end_ts}")

    has_identify_ids = F.col("repo.id").isNotNull() & F.col("actor.id").isNotNull()
    date_between = (F.col("ingested_at") >= F.lit(start_ts)) & (
        F.col("ingested_at") < F.lit(end_ts)
    )
    type_eq_watch_event = F.col("type") == F.lit("WatchEvent")

    source_df = spark.read.table(bronze_gharchive_events_table_name).filter(
        has_identify_ids & date_between & type_eq_watch_event
    )

    source_df = source_df.withColumn(
        "action", F.get_json_object("payload_raw", "$.action")
    )

    result_df = source_df.select(
        F.col("id").alias("event_id"),
        F.col("repo.id").alias("repo_id"),
        F.col("actor.id").alias("actor_id"),
        F.col("type").alias("event_type"),
        F.col("action"),
        F.col("created_at"),
        F.col("ingested_at"),
        F.col("ingested_date"),
        F.col("ingested_hour"),
    )

    if not spark.catalog.tableExists(target_silver_watch_events_table_name):
        (
            result_df.writeTo("nessie.gitsight.silver.watch_events")
            .tableProperty("format-version", "2")
            .partitionedBy(F.col("ingested_date"), F.col("ingested_hour"))
            .createOrReplace()
        )
    else:
        (
            result_df.writeTo("nessie.gitsight.silver.watch_events")
            .partitionedBy(F.col("ingested_date"), F.col("ingested_hour"))
            .overwritePartitions()
        )


if __name__ == "__main__":
    spark_session = SparkSessionFactory.create_session(
        "ExtractWatchEventsFromBronzeJob"
    )

    args = parse_required_args(["data_interval_start", "data_interval_end"])

    transform_silver_watch_events_from_bronze_job(
        spark=spark_session,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
    )
