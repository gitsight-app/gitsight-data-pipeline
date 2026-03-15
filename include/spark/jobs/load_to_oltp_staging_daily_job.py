from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from include.spark.common.decorators import spark_session_manager
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args
from include.spark.utils.jdbc_utils import get_jdbc_config

source_gold_repo_metrics_table_name = "nessie.gitsight.gold.repo_metrics_daily"


@spark_session_manager
def load_to_oltp_staging_job(
    *,
    spark: SparkSession,
    source_table_name,
    staging_table_name,
    target_date,
    date_condition_col_name,
    logger,
    **kwargs,
):

    source_df = spark.read.table(source_table_name).where(
        F.col(date_condition_col_name) == F.lit(target_date)
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
    spark_session = SparkSessionFactory.create_session("LoadToOltpStagingJob")
    args = parse_required_args(
        [
            "target_date",
            "target_table_name",
            "source_table_name",
            "date_condition_col_name",
        ]
    )

    load_to_oltp_staging_job(
        spark=spark_session,
        target_date=args.target_date,
        staging_table_name=args.target_table_name,
        source_table_name=args.source_table_name,
        date_condition_col_name=args.date_condition_col_name,
    )
