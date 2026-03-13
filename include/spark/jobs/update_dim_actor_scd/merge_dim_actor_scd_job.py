import pendulum
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.connect.dataframe import DataFrame

from include.spark.common.decorators import spark_session_manager
from include.spark.common.session_factory import SparkSessionFactory
from include.spark.utils.arg_parse_utils import parse_required_args
from include.spark.utils.country_utils import (
    SUBDIVISION_TO_COUNTRY,
    get_extract_country_udf,
)

source_actor_detail_raw_table_name = "nessie.gitsight.bronze.actor_detail_raw"
target_actor_detail_scd_table_name = "nessie.gitsight.silver.actor_detail_scd"
actor_detail_view_name = "actor_detail_view"


@spark_session_manager
def merge_dim_actor_detail_scd_job(
    spark: SparkSession, *, target_datetime: str, **kwargs
):
    clean_regex = r"[^\p{L}\p{N}\s\-_&,().]"
    at_regex = r"^@"
    target_ts = pendulum.parse(target_datetime)
    target_date = target_ts.date()
    target_hour = target_ts.hour

    in_target_date = (F.col("ingested_date") == F.lit(target_date)) & (
        F.col("ingested_hour") == F.lit(target_hour)
    )

    actor_detail_raw_df = spark.read.table(source_actor_detail_raw_table_name).filter(
        in_target_date
    )

    window_spec = Window.partitionBy("user_id").orderBy(F.col("ingested_at").desc())

    actor_detail_raw_df = (
        actor_detail_raw_df.select("*", F.row_number().over(window_spec).alias("rn"))
        .filter(F.col("rn") == F.lit(1))
        .drop("rn")
    )

    processed_df = (
        actor_detail_raw_df.withColumn(
            "company", F.regexp_replace(F.col("company"), at_regex, "")
        )
        .withColumn("company", F.regexp_replace(F.col("company"), clean_regex, ""))
        .withColumn("company", F.trim(F.initcap(F.col("company"))))
    )

    subdivision_bc = spark.sparkContext.broadcast(SUBDIVISION_TO_COUNTRY)

    processed_df = processed_df.withColumn(
        "country_extracted", get_extract_country_udf(subdivision_bc)(F.col("location"))
    )
    current_batch_time = F.to_timestamp(F.lit(target_ts))
    max_future_time = F.to_timestamp(F.lit("9999-12-31 23:59:59"))

    processed_df = (
        processed_df.withColumn("valid_from", current_batch_time)
        .withColumn("valid_to", max_future_time)
        .withColumn("is_current", F.lit(True))
    )

    processed_df = processed_df.withColumn(
        "hash_diff",
        F.sha2(
            F.concat_ws(
                "|",
                F.coalesce(F.col("name"), F.lit("")),
                F.coalesce(F.col("country_extracted"), F.lit("")),
                F.coalesce(F.col("company"), F.lit("")),
            ),
            256,
        ),
    )

    if not spark.catalog.tableExists(target_actor_detail_scd_table_name):
        _create_actor_detail_table(df=processed_df)
    else:
        source_table_name = "source_view"
        processed_df.createOrReplaceTempView(source_table_name)

        _query_merge_dim_actor_scd(
            spark, source_table_name, target_actor_detail_scd_table_name
        )

    _create_actor_detail_view(spark, actor_detail_view_name)


def _create_actor_detail_table(df: DataFrame):
    (
        df.writeTo(target_actor_detail_scd_table_name)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.metadata.delete-after-commit.enabled", "true")
        .partitionedBy(F.col("is_current"))
        .create()
    )


def _query_merge_dim_actor_scd(spark: SparkSession, source_name: str, target_name: str):
    spark.sql(f"""
        MERGE INTO {target_name} AS target
        USING (
            SELECT
            s.*
            , CAST(NULL AS INT) AS merge_key
            FROM {source_name} s
            LEFT JOIN {target_name} t
                ON s.user_id = t.user_id
                    AND t.is_current = true
            WHERE
                t.user_id IS NULL
                    OR s.hash_diff != t.hash_diff
            UNION ALL
            SELECT
                s.*
                , s.user_id AS merge_key
            FROM {source_name} s
            JOIN {target_name} t
                ON s.user_id = t.user_id
                    AND t.is_current = true
            WHERE
                s.hash_diff != t.hash_diff
        ) AS source
        ON target.user_id = source.merge_key
        AND target.is_current = true

        WHEN MATCHED
            AND source.valid_from > target.valid_from THEN
            UPDATE SET
                target.valid_to = source.valid_from,
                target.is_current = false

        WHEN NOT MATCHED THEN
            INSERT (
                user_id
                , login
                , name
                , company
                , location
                , followers_count
                , following_count
                , user_created_at
                , user_updated_at
                , ingested_at
                , ingested_date
                , ingested_hour
                , country_extracted
                , valid_from
                , valid_to
                , is_current
                , hash_diff
            )
            VALUES (
                source.user_id
                , source.login
                , source.name
                , source.company
                , source.location
                , source.followers_count
                , source.following_count
                , source.user_created_at
                , source.user_updated_at
                , source.ingested_at
                , source.ingested_date
                , source.ingested_hour
                , source.country_extracted
                , source.valid_from
                , source.valid_to
                , true
                , source.hash_diff
            )
    """)  # noqa: S608


def _create_actor_detail_view(spark: SparkSession, view_name: str):
    spark.sql(f"""
        CREATE OR REPLACE VIEW {view_name} AS (
        SELECT *
        FROM {target_actor_detail_scd_table_name}
        WHERE is_current = true
    )
    """)  # noqa: S608


if __name__ == "__main__":
    args = parse_required_args(["target_datetime"])
    spark_session = SparkSessionFactory.create_session("MergeDimActorDetailSCDJob")

    merge_dim_actor_detail_scd_job(
        spark_session,
        target_datetime=args.target_datetime,
    )
