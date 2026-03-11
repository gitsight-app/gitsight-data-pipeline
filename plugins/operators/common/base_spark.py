from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.context import Context
from common.spark.enricher_builder import SparkConfigBuilder
from common.spark.spark_enrichers import BaseEnricher, SparkConfigEnricher


class BaseSparkOperator(SparkSubmitOperator):
    def __init__(
        self,
        *,
        spark_config_conn_id: str = "spark_config_default",
        enrichers: list[BaseEnricher],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conf = self.conf or {}
        self.enrichers = [SparkConfigEnricher(conn_id=spark_config_conn_id), *enrichers]

    def execute(self, context: Context) -> None:
        builder = SparkConfigBuilder()
        builder.apply(self.enrichers)
        self.conf.update(builder.build())
        super().execute(context)
