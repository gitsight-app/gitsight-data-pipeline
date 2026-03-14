import pendulum
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from include.spark.common.decorators import spark_session_manager
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args
from include.spark.utils.condition_utils import get_ingested_at_between_condition

source_repo_meta_table_name = "nessie.gitsight.bronze.actor_meta"
target_repo_master_table_name: str = "nessie.gitsight.silver.actor_master"


@spark_session_manager
def load_actor_master_to_silver_job(
    *, spark: SparkSession, data_interval_start, data_interval_end, logger, **kwargs
):
    start_ts = pendulum.parse(data_interval_start).start_of("hour")
    end_ts = pendulum.parse(data_interval_end).start_of("hour")

    has_actor_id = F.col("actor_id").isNotNull()

    source_df = spark.read.table(source_repo_meta_table_name).where(
        has_actor_id & get_ingested_at_between_condition(start_ts, end_ts)
    )

    window = Window.partitionBy("actor_id").orderBy(
        F.desc("created_at"), F.desc("ingested_at")
    )

    dedup_source_df = (
        (
            source_df.withColumn("row_number", F.row_number().over(window))
            .filter(F.col("row_number") == F.lit(1))
            .drop(F.col("row_number"))
        )
        .coalesce(2)
        .cache()
    )

    if not spark.catalog.tableExists(target_repo_master_table_name):
        (
            dedup_source_df.writeTo(target_repo_master_table_name)
            .tableProperty("format-version", "2")
            .tableProperty("write.distribution-mode", "hash")
            .tableProperty("write.sort-order", "actor_id ASC")
            .createOrReplace()
        )
    else:
        dedup_source_df.createOrReplaceTempView("source_df")

        spark.sql(f"""
        MERGE INTO {target_repo_master_table_name} AS target
        USING source_df AS source
        ON target.actor_id = source.actor_id
        WHEN MATCHED AND target.created_at < source.created_at THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """)

    dedup_source_df.unpersist()


if __name__ == "__main__":
    spark_session = SparkSessionFactory.create_session("LoadSilverMasterRepoJob")
    args = parse_required_args(["data_interval_start", "data_interval_end"])

    load_actor_master_to_silver_job(
        spark=spark_session,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
    )
