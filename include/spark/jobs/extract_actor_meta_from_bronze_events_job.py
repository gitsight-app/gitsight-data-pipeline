from pyspark.sql import functions as F

from include.spark.common.decorators import spark_session_manager
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args
from include.spark.utils.time_utils import get_timestamp_col

actor_meta_table_name = "nessie.gitsight.bronze.actor_meta"


@spark_session_manager
def extract_actor_meta_from_bronze_events_job(
    *, spark, data_interval_start, data_interval_end, logger, **kwargs
):
    start_ts = get_timestamp_col(data_interval_start)
    end_ts = get_timestamp_col(data_interval_end)

    events_df = spark.read.table("nessie.gitsight.bronze.gharchive_events").where(
        (F.col("ingested_at") >= start_ts) & (F.col("ingested_at") < end_ts)
    )

    repo_meta_df = events_df.select(
        F.col("actor.id").alias("actor_id"),
        F.col("actor.avatar_url").alias("avatar_url"),
        F.col("actor.display_login").alias("actor_display_login"),
        F.col("actor.gravatar_id").alias("actor_gravatar_id"),
        F.col("actor.login").alias("actor_login"),
        F.col("actor.url").alias("actor_url"),
        F.col("created_at").alias("created_at"),
        F.col("ingested_at").alias("ingested_at"),
        F.col("ingested_date").alias("ingested_date"),
    )

    logger.info(
        "[INFO] detected {} records to be written into actor meta table".format(
            repo_meta_df.count()
        )
    )  # noqa: E501

    if not spark.catalog.tableExists(actor_meta_table_name):
        (
            repo_meta_df.writeTo(actor_meta_table_name)
            .tableProperty("format-version", "2")
            .partitionedBy(F.col("ingested_date"))
            .create()
        )
    else:
        (
            repo_meta_df.writeTo(actor_meta_table_name)
            .partitionedBy(F.col("ingested_date"))
            .append()
        )


if __name__ == "__main__":
    spark_session = SparkSessionFactory.create_session(
        "ExtractActorMetaFromBronzeEventsJob"
    )
    args = parse_required_args(["data_interval_start", "data_interval_end"])

    extract_actor_meta_from_bronze_events_job(
        spark=spark_session,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
    )
