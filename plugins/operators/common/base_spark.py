from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.context import Context
from common.spark.enricher_builder import SparkConfigBuilder
from common.spark.spark_enrichers import BaseEnricher

from include.spark.common.dependencies import SPARK_PACKAGES


class BaseSparkOperator(SparkSubmitOperator):
    def __init__(self, *, enrichers: list[BaseEnricher], **kwargs):
        super().__init__(packages=SPARK_PACKAGES, **kwargs)
        self.conf = self.conf or {}
        self.conf.setdefault("spark.driver.extraClassPath", "/opt/airflow/jars/*")
        self.conf.setdefault("spark.executor.extraClassPath", "/opt/airflow/jars/*")
        self.enrichers = enrichers

    def execute(self, context: Context) -> None:
        builder = SparkConfigBuilder()
        builder.apply(self.enrichers)
        self.conf.update(builder.build())
        super().execute(context)
