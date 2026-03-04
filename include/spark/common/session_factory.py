from pyspark import SparkConf
from pyspark.sql import SparkSession

from include.spark.common.dependencies import SPARK_EXTENSIONS, SPARK_PACKAGES


class SparkSessionFactory:
    """
    Create Spark Session with default configurations and required dependencies for Spark jobs

    If using without Airflow(Connections, Variables),
    need to use extra_conf to inject catalog, AWS and other necessary configurations
    """  # noqa: E501

    @staticmethod
    def create_session(app_name: str, extra_conf: dict | None = None):

        default_conf = {
            "spark.sql.session.timeZone": "UTC",
            "spark.sql.adaptive.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.jars.packages": SPARK_PACKAGES,
            "spark.sql.extensions": SPARK_EXTENSIONS,
            "spark.jars.driver.class": "org.postgresql.Driver",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.sql.legacy.parquet.nanosAsLong": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.dynamicAllocation.enabled": "false",
            "spark.redaction.string.regex": "(?i)secret|password|token|access[.]key",
        }

        if extra_conf:
            default_conf.update(extra_conf)

        conf = SparkConf().setAppName(app_name)
        for key, value in default_conf.items():
            if value is not None:
                conf.set(key, value)

        spark = SparkSession.builder.config(conf=conf).getOrCreate()

        return spark
