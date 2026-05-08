from enum import Enum

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)


class RefActionType(Enum):
    CREATE = "create"
    DELETE = "delete"
    MERGE = "merge"


class NessieRefOperator(SparkKubernetesOperator):
    def __init__(
        self,
        *,
        application_file="spark/jobs/nessie_branch/application.yaml",
        namespace="spark-applications",
        action: RefActionType = RefActionType.CREATE,
        **kwargs,
    ):
        super().__init__(
            application_file=application_file,
            namespace=namespace,
            params={"action": action.value},
            **kwargs,
        )
