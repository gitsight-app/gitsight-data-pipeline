import pendulum
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from include.spark.common.decorators import spark_session_manager
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.jobs.update_dim_actor_scd.worker_logic import (
    fetch_users_partition,
)
from include.spark.utils.arg_parse_utils import parse_required_args

source_unified_events_view_name = "nessie.gitsight.silver.unified_events"
target_actor_detail_raw_table_name = "nessie.gitsight.bronze.actor_detail_raw"

schema = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("name", StringType(), True),
        StructField("company", StringType(), True),
        StructField("location", StringType(), True),
        StructField("followers_count", IntegerType(), True),
        StructField("following_count", IntegerType(), True),
        StructField("user_created_at", TimestampType(), True),
        StructField("user_updated_at", TimestampType(), True),
    ]
)


@spark_session_manager
def fetch_actor_detail_to_raw_job(
    spark: SparkSession,
    *,
    target_datetime: str,
    github_token: str,
    logger,
    **kwargs,
):
    datetime = pendulum.parse(target_datetime)
    target_date = datetime.to_date_string()
    target_hour = datetime.hour

    logger.info(
        f"Start Fetching Dimension Active Actor Detail (date: {target_date}, hour: {target_hour})"  # noqa: E501
    )
    between_target_ingested = (F.col("ingested_date") == F.lit(target_date)) & (
        F.col("ingested_hour") == F.lit(target_hour)
    )

    source_df = spark.read.table(source_unified_events_view_name).filter(
        between_target_ingested
    )
    source_df = source_df.repartition(5, F.col("actor_id"))

    distinct_actor_ids = source_df.select(F.col("actor_id").alias("user_id")).distinct()

    token_bc = spark.sparkContext.broadcast(github_token)
    batch_size_bc = spark.sparkContext.broadcast(50)

    actor_detail_raw_rdd = distinct_actor_ids.rdd.mapPartitions(
        lambda iterator: fetch_users_partition(
            iterator,
            token_bc=token_bc,
            batch_size_bc=batch_size_bc,
        )
    )
    actor_detail_raw_df = actor_detail_raw_rdd.toDF(schema)

    actor_detail_raw_df = actor_detail_raw_df.withColumn("ingested_at", F.lit(datetime))

    actor_detail_raw_df = actor_detail_raw_df.select(
        "*",
        F.to_date(F.col("ingested_at")).alias("ingested_date"),
        F.hour(F.col("ingested_at")).alias("ingested_hour"),
    )

    if not spark.catalog.tableExists(target_actor_detail_raw_table_name):
        (
            actor_detail_raw_df.writeTo(target_actor_detail_raw_table_name)
            .tableProperty("format-version", "2")
            .partitionedBy(F.col("ingested_date"), F.col("ingested_hour"))
            .create()
        )
    else:
        (
            actor_detail_raw_df.writeTo(target_actor_detail_raw_table_name)
            .partitionedBy(F.col("ingested_date"), F.col("ingested_hour"))
            .append()
        )


if __name__ == "__main__":
    args = parse_required_args(["target_datetime", "github_token"])
    spark_session = SparkSessionFactory.create_session(
        "FetchDimActorDetailRawJob", extra_conf={"spark.sql.adaptive.enabled": "false"}
    )

    fetch_actor_detail_to_raw_job(
        spark_session,
        target_datetime=args.target_datetime,
        github_token=args.github_token,
    )
