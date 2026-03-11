from common.spark.spark_enrichers import (
    AwsEnricher,
    CatalogEnricher,
)
from operators.common.base_spark import BaseSparkOperator


class CommonLakeSparkOperator(BaseSparkOperator):
    """
    Lake Spark Operator
    Include: AWS Conn, Nessie Catalog
    """

    template_fields = (*BaseSparkOperator.template_fields,)

    def __init__(
        self,
        *,
        application,
        application_args=None,
        aws_conn_id="aws_default",
        catalog_conn_id="catalog_default",
        **kwargs,
    ):
        super().__init__(
            application=application,
            application_args=application_args,
            enrichers=[
                AwsEnricher(aws_conn_id=aws_conn_id),
                CatalogEnricher(catalog_conn_id=catalog_conn_id),
            ],
            **kwargs,
        )

    def execute(self, context) -> None:

        return super().execute(context)
