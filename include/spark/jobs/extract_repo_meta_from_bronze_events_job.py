from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from include.spark.common.decorators import spark_session_manager
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args
from include.spark.utils.time_utils import get_timestamp_col

repo_meta_table_name = "nessie.gitsight.bronze.repo_meta"


@spark_session_manager
def extract_repo_meta_from_bronze_events_job(
    *, spark: SparkSession, data_interval_start, data_interval_end, logger, **kwargs
):
    start_ts = get_timestamp_col(data_interval_start)
    end_ts = get_timestamp_col(data_interval_end)

    events_df = spark.read.table("nessie.gitsight.bronze.gharchive_events").where(
        (F.col("ingested_at") >= start_ts) & (F.col("ingested_at") < end_ts)
    )

    repo_meta_df = events_df.select(
        F.col("repo.id").alias("repo_id"),
        F.col("repo.name").alias("repo_name"),
        F.col("repo.url").alias("repo_url"),
        F.col("created_at").alias("created_at"),
        F.col("ingested_at").alias("ingested_at"),
        F.col("ingested_date").alias("ingested_date"),
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
            .partitionedBy(F.col("ingested_date"))
            .create()
        )
    else:
        (
            repo_meta_df.writeTo(repo_meta_table_name)
            .partitionedBy(F.col("ingested_date"))
            .append()
        )


if __name__ == "__main__":
    spark_session = SparkSessionFactory.create_session(
        "ExtractRepoMetaFromBronzeEventsJob"
    )
    args = parse_required_args(["data_interval_start", "data_interval_end"])

    extract_repo_meta_from_bronze_events_job(
        spark=spark_session,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
    )
