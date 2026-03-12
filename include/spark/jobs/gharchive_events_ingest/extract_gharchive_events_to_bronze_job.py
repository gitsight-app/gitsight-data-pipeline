from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from include.spark.common.decorators import spark_session_manager
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args

gharchive_events_table_name = "nessie.gitsight.bronze.gharchive_events"


@spark_session_manager
def extract_gharchive_events_to_bronze(
    *, spark: SparkSession, source_path, data_interval_start, **kwargs
):
    raw_df = spark.read.format("json").load(source_path)

    raw_df_flatting_payload = raw_df.withColumn(
        "payload_raw", F.to_json("payload")
    ).drop("payload")

    raw_df_with_ingested_at = raw_df_flatting_payload.withColumn(
        "ingested_at", F.to_timestamp(F.lit(data_interval_start))
    )

    raw_df_with_partition_cols = raw_df_with_ingested_at.withColumn(
        "ingested_date", F.to_date("ingested_at")
    ).withColumn("ingested_hour", F.hour("ingested_at"))

    if not spark.catalog.tableExists(gharchive_events_table_name):
        (
            raw_df_with_partition_cols.writeTo(gharchive_events_table_name)
            .partitionedBy(
                F.col("ingested_date"),
                F.col("ingested_hour"),
            )
            .tableProperty("format-version", "2")
            .create()
        )
    else:
        (
            raw_df_with_partition_cols.writeTo(gharchive_events_table_name)
            .partitionedBy(
                F.col("ingested_date"),
                F.col("ingested_hour"),
            )
            .overwritePartitions()
        )


if __name__ == "__main__":
    spark_session = SparkSessionFactory.create_session("ExtractGhArchiveEventsToBronze")
    args = parse_required_args(["source_path", "data_interval_start"])

    extract_gharchive_events_to_bronze(
        spark=spark_session,
        source_path=args.source_path,
        data_interval_start=args.data_interval_start,
    )
