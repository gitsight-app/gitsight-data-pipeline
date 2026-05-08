from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)


class GXTableValidateOperator(SparkKubernetesOperator):
    def __init__(
        self,
        *,
        application_file="spark/jobs/gx_table_validation/application.yaml",
        namespace="spark-applications",
        source_table_name,
        date_column,
        identify_name,
        **kwargs,
    ):
        super().__init__(
            application_file=application_file,
            namespace=namespace,
            **kwargs,
            params={
                "source_table_name": source_table_name,
                "date_column": date_column,
                "suite_name": f"{identify_name}_suite",
                "checkpoint_name": f"{identify_name}_checkpoint",
                "data_asset_name": f"{identify_name}",
            },
        )
