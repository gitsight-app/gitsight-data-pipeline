import pendulum
from pyspark.sql import SparkSession

from include.spark.common.decorators import spark_session_manager
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args
from include.spark.utils.condition_utils import get_ingested_at_between_condition
from include.spark.utils.jdbc_utils import get_jdbc_config

source_gold_repo_metrics_table_name = "nessie.gitsight.gold.repo_metrics_hourly"


@spark_session_manager
def load_oltp_gold_repo_metrics_hourly_to_staging_job(
    *,
    spark: SparkSession,
    data_interval_start,
    data_interval_end,
    staging_table_name,
    logger,
    **kwargs,
):
    start_ts = pendulum.parse(data_interval_start).start_of("hour")
    end_ts = pendulum.parse(data_interval_end).start_of("hour")

    source_df = spark.read.table(source_gold_repo_metrics_table_name).where(
        get_ingested_at_between_condition(start_ts, end_ts)
    )

    jdbc_config = get_jdbc_config(spark.conf)

    (
        source_df.coalesce(1)
        .write.format("jdbc")
        .option("url", jdbc_config.url)
        .option("dbtable", staging_table_name)
        .option("user", jdbc_config.user)
        .option("password", jdbc_config.password)
        .option("driver", jdbc_config.driver)
        .option("batchsize", 5000)
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


if __name__ == "__main__":
    spark_session = SparkSessionFactory.create_session(
        "MergeToOltpGoldRepoMetricsHourlyJob"
    )
    args = parse_required_args(
        ["data_interval_start", "data_interval_end", "staging_table_name"]
    )

    load_oltp_gold_repo_metrics_hourly_to_staging_job(
        spark=spark_session,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
        staging_table_name=args.staging_table_name,
    )
