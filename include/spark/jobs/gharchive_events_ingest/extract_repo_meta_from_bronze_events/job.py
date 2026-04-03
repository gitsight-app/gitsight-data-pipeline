import logging

import pendulum
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args
from include.spark.utils.condition_utils import get_ingested_at_between_condition
from include.spark.utils.logger_utils import get_logger

repo_meta_table_name = "nessie.gitsight.bronze.repo_meta"


def extract_repo_meta_from_bronze_events_job(
    *, spark: SparkSession, data_interval_start, data_interval_end, logger
):
    start_ts = pendulum.parse(data_interval_start).start_of("hour")
    end_ts = pendulum.parse(data_interval_end).start_of("hour")

    events_df = spark.read.table("nessie.gitsight.bronze.gharchive_events").where(
        get_ingested_at_between_condition(start_ts, end_ts)
    )

    repo_meta_df = events_df.select(
        F.col("repo.id").alias("repo_id"),
        F.col("repo.name").alias("repo_name"),
        F.col("repo.url").alias("repo_url"),
        F.col("created_at").alias("created_at"),
        F.col("ingested_at").alias("ingested_at"),
    )

    logger.info(
        "[INFO] detected {} records to be written into repo_meta table".format(
            repo_meta_df.count()
        )
    )  # noqa: E501

    if not spark.catalog.tableExists(repo_meta_table_name):
        (
            repo_meta_df.writeTo(repo_meta_table_name)
            .tableProperty("format-version", "2")
            .partitionedBy(F.hours("ingested_at"))
            .create()
        )
    else:
        (repo_meta_df.writeTo(repo_meta_table_name).append())


if __name__ == "__main__":
    spark_session = SparkSessionFactory.create_session(
        "ExtractRepoMetaFromBronzeEventsJob"
    )
    args = parse_required_args(["data_interval_start", "data_interval_end"])

    logger = logging.getLogger(__file__)

    extract_repo_meta_from_bronze_events_job(
        spark=spark_session,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
        logger=get_logger("ExtractRepoMetaFromBronzeEventsJob"),
    )
